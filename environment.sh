sudo apt-get update
sudo apt install git
git config --global user.name "ipqhjjybj"
git config --global user.email "250657661@qq.com"
sudo apt-get install erlang-nox
sudo apt-get update
sudo apt-get install rabbitmq-server

sudo rabbitmq-plugins enable rabbitmq_management
sudo rabbitmqctl add_user  admin  admin
sudo rabbitmqctl set_user_tags admin administrator
sudo rabbitmqctl set_permissions -p / admin '.*' '.*' '.*'

sudo apt install redis-server
# 中间如果碰到问题看这个
https://www.cnblogs.com/bymo/p/9046586.html

sudo apt-get install mysql-server
sudo mysql_secure_installation

sudo apt-get install mongodb

sudo mysql -u root
use mysql;
[mysql] update user set plugin='mysql_native_password' where User='root';
[mysql] flush privileges;

mysql -uroot -p
[mysql] use mysql;
[mysql] set global validate_password_policy=0;
[mysql] set global validate_password_mixed_case_count=0;
[mysql] set global validate_password_mixed_case_count=0;
[mysql] set global validate_password_number_count=3;
[mysql] set global validate_password_special_char_count=0;
[mysql] set global validate_password_length=3;
[mysql] update mysql.user set authentication_string=password('8btc-quant.now
') where user='root' and Host = 'localhost';
[mysql] flush privileges;


git clone git@github.com:Bytom/tumbler.git ./tumbler
git config --global credential.helper store

sudo apt install python3-pip
sudo apt-get install libmysqlclient-dev python3-dev
sudo pip3 install -r requirements.txt

如果碰到问题
dpkg --list|grep mysql
sudo apt-get remove libmysqlclient20

sudo apt-get install build-essential python3-dev libssl-dev libffi-dev libxml2 libxml2-dev libxslt1-dev zlib1g-dev
python3.6 -m pip install numpy -i https://pypi.doubanio.com/simple
或者
sudo apt-get install python-numpy

talib安装
wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz
tar -zxvf ta-lib-0.4.0-src.tar.gz

git clone git@github.com:Bytom/mov-mmdk.git ./mov-mmdk

cd ta-lib
sudo ./configure
sudo make
sudo make install
sudo pip3 install ta-lib

Ta-lib Error
sudo -s
echo "include /usr/local/lib" >> /etc/ld.so.conf
ldconfig

# 上面不成功执行以下操作
# sudo pip3 install https://github.com/mrjbq7/ta-lib/archive/TA_Lib-0.4.8.zip
# sudo pip3 install https://github.com/mrjbq7/ta-lib/zipball/master   #上式不成功用这个


Redis
redis-cli
config set stop-writes-on-bgsave-error no


# ----------



[Redis]
# Mac
sudo vim /usr/local/redis-5.0.5/redis.conf

# ubuntu
sudo vim /etc/redis/redis.conf
requirepass  8btc-quant.now

sudo service redis-server restart

redis-cli
	auth 8btc-quant.now
	set 1 1
	get 1



[mongo]
sudo vim /etc/mongodb.conf
auth = true

mongo
use admin
db.createUser({user:"root",pwd:"8btc-quant.now",roles:["root"]}) 


sudo service mongodb stop
sudo service mongodb start

mongo
use admin
db.auth("root", "8btc-quant.now")



如果启动失败:
sudo rm /var/lib/mongodb/mongod.lock
mongod --repair
sudo service mongodb start


[mysql]
sudo service mysql status

sudo service mysql stop

sudo service mysql start

修改密码
update mysql.user set authentication_string=password('8btc-quant.now') where user='root' and Host = 'localhost';
update mysql.user set authentication_string=password('123456') where user='root' and Host = 'localhost';
flush privileges;
