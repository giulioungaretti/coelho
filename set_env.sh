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
