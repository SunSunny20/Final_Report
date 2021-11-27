FROM ubuntu:20.04
RUN apt-get update
RUN apt-get install -y ssh wget openjdk-8-jdk
RUN apt-get install openssh-server
RUN wget --no-verbose https://dlcdn.apache.org/hadoop/common/hadoop-3.2.2/hadoop-3.2.2.tar.gz && tar -xvzf hadoop-3.2.2.tar.gz && mv hadoop-3.2.2 hadoop && rm hadoop-3.2.2.tar.gz

ENV HDFS_NAMENODE_USER="root"
ENV HDFS_DATANODE_USER="root"
ENV HDFS_SECONDARYNAMENODE_USER="root"
ENV YARN_RESOURCEMANAGER_USER="root"
ENV YARN_NODEMANAGER_USER="root"
COPY config ~/.ssh/
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
RUN cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
RUN chmod 0600 ~/.ssh/authorized_keys

#ENV JAVA_HOME=$(dirname $(dirname $(readlink -f $(which javac))))
RUN echo "export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which javac))))\n$(cat hadoop/etc/hadoop/hadoop-env.sh)" > hadoop/etc/hadoop/hadoop-env.sh
COPY core-site.xml hadoop/etc/hadoop/
COPY hdfs-site.xml hadoop/etc/hadoop/
COPY mapred-site.xml hadoop/etc/hadoop/
COPY yarn-site.xml hadoop/etc/hadoop/
RUN echo 'Y' | hadoop/bin/hdfs namenode -format -y
CMD ["/usr/sbin/sshd", "-D"]