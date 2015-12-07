#! /bin/sh
#
# set_env.sh
# Copyright (C) 2015 giulio <giulioungaretti@me.com>
#
# Distributed under terms of the MIT license.
#
host='192.168.33.10'
port='5672'
user='guest'
pass='guest'
vhost=''
rabbitMqAddres='amqp://'$user':'$pass'@'$host':'$port'/'$vhost
export rabbitMqAddres


# exchange name
export exchange_name="events"
export exchange_type="direct"
# if QoS = 0 it is disabled which is what we mostly want
export QoS=0


#local  rabbit mq clusta
host='192.168.33.11'
port='5672'
user='guest'
pass='guest'
vhost=''
local_rabbitMQ_addres='amqp://'$user':'$pass'@'$host':'$port'/'$vhost
export local_rabbitMQ_addres
# exchange name
export local_exchange_name="events"
export local_exchange_type="direct"
# if QoS = 0 it is disabled which is what we mostly want
export QoS=0














