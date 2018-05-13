import tensorflow as tf
import math,logging
from pprint import  pprint
from time import time

class skipgram(object):
    '''
    skipgram model - refer Mikolov et al (2013)
    '''

    def __init__(self,num_graphs,num_subgraphs,learning_rate,embedding_size,
                 num_negsample,num_steps,corpus):
        self.num_graphs = num_graphs
        self.num_subgraphs = num_subgraphs
        self.embedding_size = embedding_size
        self.num_negsample = num_negsample
        self.learning_rate = learning_rate
        self.num_steps = num_steps
        self.corpus = corpus
        self.graph, self.batch_inputs, self.batch_labels,self.normalized_embeddings,\
        self.loss,self.optimizer = self.trainer_initial()


    def trainer_initial(self):
        graph = tf.Graph()
        with graph.as_default():

            path = '/home/pogorelov/model_tf/'
            sess = tf.Session()
            saver = tf.train.import_meta_graph(path + 'model.meta')
            saver.restore(sess, tf.train.latest_checkpoint(path))

            batch_inputs = tf.placeholder(tf.int32, shape=([None, ]))
            batch_labels = tf.placeholder(tf.int64, shape=([None, 1]))

            graph_embeddings = tf.Variable(
                    tf.random_uniform([self.num_graphs, self.embedding_size], -0.5 / self.embedding_size, 0.5/self.embedding_size))

            batch_graph_embeddings = tf.nn.embedding_lookup(graph_embeddings, batch_inputs) #hiddeb layer

            weights = tf.Variable(graph.get_tensor_by_name("weights:0"), trainable=False, name='weights')  # output layer wt
            biases = tf.Variable(graph.get_tensor_by_name("bias:0"), trainable=False)  # output layer biases

            #negative sampling part
            loss = tf.reduce_mean(
                tf.nn.nce_loss(weights=weights,
                               biases=biases,
                               labels=batch_labels,
                               inputs=batch_graph_embeddings,
                               num_sampled=self.num_negsample,
                               num_classes=self.num_subgraphs,
                               sampled_values=tf.nn.fixed_unigram_candidate_sampler(
                                   true_classes=batch_labels,
                                   num_true=1,
                                   num_sampled=self.num_negsample,
                                   unique=True,
                                   range_max=self.num_subgraphs,
                                   distortion=0.75,
                                   unigrams=self.corpus.subgraph_id_freq_map_as_list)#word_id_freq_map_as_list is the
                               # frequency of each word in vocabulary
                               ))

            global_step = tf.Variable(0, trainable=False)
            learning_rate = tf.train.exponential_decay(self.learning_rate,
                                                       global_step, 100000, 0.96, staircase=True) #linear decay over time

            learning_rate = tf.maximum(learning_rate,0.001) #cannot go below 0.001 to ensure at least a minimal learning

            optimizer = tf.train.GradientDescentOptimizer(learning_rate).minimize(loss,global_step=global_step)

            norm = tf.sqrt(tf.reduce_mean(tf.square(graph_embeddings), 1, keep_dims=True))
            normalized_embeddings = tf.Variable(graph_embeddings / norm, name='normalized_embeddings')

            self.saver = tf.train.Saver()
        return graph,batch_inputs, batch_labels, normalized_embeddings, loss, optimizer



    def train(self,corpus,batch_size):
        with tf.Session(graph=self.graph,
                        config=tf.ConfigProto(log_device_placement=True,allow_soft_placement=False)) as sess:

            count = 0
            tf.summary.scalar('loss', self.loss)
            # tf.summary.scalar('count', count)

            log_path = '/home/pogorelov/work/logs_tensorboard_2'
            # log_path = '/Users/grigoriipogorelov/Desktop/KL_graph_embeddings/logs_tensorboard'
            train_writer = tf.summary.FileWriter(log_path, sess.graph)

            merge = tf.summary.merge_all()

            init = tf.global_variables_initializer()
            sess.run(init)

            loss = 0
            for i in xrange(self.num_steps):
                t0 = time()
                step = 0
                while corpus.epoch_flag == False:
                    batch_data, batch_labels = corpus.generate_batch_from_file(batch_size)# get (target,context) wordid tuples

                    feed_dict = {self.batch_inputs:batch_data,self.batch_labels:batch_labels}
                    summary, _,loss_val = sess.run([merge, self.optimizer,self.loss],feed_dict=feed_dict)

                    train_writer.add_summary(summary, count)

                    loss += loss_val

                    if step % 100 == 0:
                        if step > 0:
                            average_loss = loss/step
                            logging.info( 'Epoch: %d : Average loss for step: %d : %f'%(i,step,average_loss))
                            count += 1
                    step += 1

                corpus.epoch_flag = False
                epoch_time = time() - t0
                logging.info('#########################   Epoch: %d :  %f, %.2f sec.  #####################' % (i, loss/step,epoch_time))
                loss = 0

            #done with training
            final_embeddings = self.normalized_embeddings.eval()
            self.saver.save(sess, "/home/pogorelov/model_tf_2/model")
        return final_embeddings
