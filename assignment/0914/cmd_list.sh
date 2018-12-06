sudo add-apt-repository -y ppa:webupd8team/java
sudo apt-get update
sudo apt-get install oracle-java8-installer
source /etc/environment
echo $JAVA_HOME
sudo apt-get install git

sudo apt-get install zsh
curl -L https://raw.github.com/robbyrussell/oh-my-zsh/master/tools/install.sh | sh

sudo apt-get -y install vim build-essential
wget http://apache.mirror.cdnetworks.com/hadoop/common/hadoop-2.7.6/hadoop-2.7.6.tar.gz
