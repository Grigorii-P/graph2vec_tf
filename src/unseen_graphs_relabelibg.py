import os,glob,ast
from time import time
import networkx as nx
from copy import deepcopy
import pickle

label_to_compressed_label_map = {}

path_to_existing_corp_and_vocab = '/home/pogorelov/subgraphs_vocab/'
path_to_new_corp_and_vocab = '/home/pogorelov/subgraphs_vocab_2/'
initial_labels = pickle.loads(open(path_to_existing_corp_and_vocab+'initial_relab.txt', 'rb').read())

get_int_node_label = lambda l: int(l.split('+')[-1])


def get_files(dirname, extn, max_files=0):
    all_files = [os.path.join(dirname, f) for f in os.listdir(dirname) if f.endswith(extn)]
    for root, dirs, files in os.walk(dirname):
        for f in files:
            if f.endswith(extn):
                all_files.append(os.path.join(root, f))

    all_files = list(set(all_files))
    all_files.sort()
    if max_files:
        return all_files[:max_files]
    else:
        return all_files

def initial_relabel(g,node_label_attr_name='label'):
    # global label_to_compressed_label_map

    nx.convert_node_labels_to_integers(g, first_label=0)  # this needs to be done for the initial interation only
    for node in g.nodes(): g.node[node]['relabel'] = {}

    for node in g.nodes():
        label = g.node[node][node_label_attr_name]
        try:
            value = initial_labels[label]
            g.node[node]['relabel'][0] = '0+' + str(value)
        except:
            g.node[node]['relabel'][0] = 'OOV-node'

    # for node in g.nodes():
    #     try:
    #         label = g.node[node][node_label_attr_name]
    #     except:
    #         # no node label referred in 'node_label_attr_name' is present, hence assigning an invalid compressd label
    #         g.node[node]['relabel'][0] = '0+0'
    #         continue
    #
    #     if not label_to_compressed_label_map.has_key(label):
    #         compressed_label = len(label_to_compressed_label_map) + 1 #starts with 1 and incremented every time a new node label is seen
    #         label_to_compressed_label_map[label] = compressed_label #inster the new label to the label map
    #         g.node[node]['relabel'][0] = '0+' + str(compressed_label)
    #     else:
    #         g.node[node]['relabel'][0] = '0+' + str(label_to_compressed_label_map[label])

    return g

def wl_relabel(g, it):
    global label_to_compressed_label_map

    prev_iter = it - 1
    for node in g.nodes():
        try:
            prev_iter_node_label = get_int_node_label(g.nodes[node]['relabel'][prev_iter])
        except:
            label_to_compressed_label_map[node] = 'OOV-graph'
            continue
        node_label = [prev_iter_node_label]
        neighbors = list(nx.all_neighbors(g, node))
        if 'OOV-node' in neighbors or 'OOV-graph' in neighbors:
            continue

        try:
            neighborhood_label = sorted([get_int_node_label(g.nodes[nei]['relabel'][prev_iter]) for nei in neighbors])
        except:
            continue
        node_neighborhood_label = tuple(node_label + neighborhood_label)
        if not label_to_compressed_label_map.has_key(node_neighborhood_label):
            label_to_compressed_label_map[node_neighborhood_label] = 'OOV-graph'
            g.node[node]['relabel'][it] = 'OOV-graph'
        else:
            g.node[node]['relabel'][it] = str(it) + '+' + str(label_to_compressed_label_map[node_neighborhood_label])

    return g

def dump_sg2vec_str (fname,max_h,g=None):
    if not g:
        g = nx.read_gexf(fname+'.tmpg')
        new_g = deepcopy(g)
        for n in g.nodes():
            del new_g.nodes[n]['relabel']
            new_g.nodes[n]['relabel'] = ast.literal_eval(g.nodes[n]['relabel'])
        g = new_g

    opfname = fname + '.g2v' + str(max_h)

    if os.path.isfile(opfname):
        return

    with open(opfname,'w') as fh:
        for n,d in g.nodes(data=True):
            for i in xrange(0, max_h+1):
                try:
                    center = d['relabel'][i]
                    if center == 'OOV-node' or center == 'OOV-graph':
                        continue
                except:
                    continue
                neis_labels_prev_deg = []
                neis_labels_next_deg = []

                try:
                    if i != 0:
                        neis_labels_prev_deg = list(set([g.node[nei]['relabel'][i-1] for nei in nx.all_neighbors(g, n)]))
                        neis_labels_prev_deg.sort()
                    NeisLabelsSameDeg = list(set([g.node[nei]['relabel'][i] for nei in nx.all_neighbors(g,n)]))
                    if i != max_h:
                        neis_labels_next_deg = list(set([g.node[nei]['relabel'][i+1] for nei in nx.all_neighbors(g,n)]))
                        neis_labels_next_deg.sort()

                    nei_list = NeisLabelsSameDeg + neis_labels_prev_deg + neis_labels_next_deg
                    nei_list = ' '.join (nei_list)

                    sentence = center + ' ' + nei_list
                    if 'OOV-graph' in sentence:
                        continue
                    print>>fh, sentence
                except:
                    continue

    if os.path.isfile(fname+'.tmpg'):
        os.system('rm '+fname+'.tmpg')
    if os.stat(opfname).st_size == 0:
        os.system('rm ' + opfname)

def wlk_relabel_and_dump_memory_version(fnames,max_h,node_label_attr_name='label'):
    global label_to_compressed_label_map

    t0 = time()
    # graphs = [nx.read_gexf(fname) for fname in fnames]
    graphs = []
    count = 0
    for fname in fnames:
        try:
            temp = nx.read_gexf(fname)
            graphs.append(temp)
        except:
            pass
    
    print 'loaded all graphs in {} sec'.format(round(time() - t0, 2))

    t0 = time()
    graphs = [initial_relabel(g,node_label_attr_name) for g in graphs]
    initial_relab = {}
    for g in graphs:
        for node in g.nodes():
            key = str(g.node[node]['relabel'])
            if not initial_relab.has_key(key):
                initial_relab[key] = 1
            else:
                initial_relab[key] = initial_relab[key] + 1
    # make dump to count OOV-nodes/graphs afterwards
    pickle.dump(initial_relab, open(path_to_new_corp_and_vocab+'initial_relab.txt', 'w'))
    print 'initial relabeling done in {} sec'.format(round(time() - t0, 2))

    for it in xrange(1, max_h + 1):
        t0 = time()
        label_to_compressed_label_map = pickle.loads(open(path_to_existing_corp_and_vocab+'degree'+str(it)+'.txt').read())
        graphs = [wl_relabel(g, it) for g in graphs]
        print 'WL iteration {} done in {} sec.'.format(it, round(time() - t0, 2))
        print 'num of WL rooted subgraphs in iter {} is {}'.format(it, len(label_to_compressed_label_map))
        to_file_dict = {k: v for k, v in label_to_compressed_label_map.items()}
        # make dump to count OOV-nodes/graphs afterwards
        pickle.dump(to_file_dict, open(path_to_new_corp_and_vocab+'degree'+str(it)+'.txt', 'w'))

    t0 = time()
    for fname, g in zip(fnames, graphs):
        dump_sg2vec_str(fname, max_h, g)
    print 'dumped sg2vec sentences in {} sec.'.format(round(time() - t0, 2))