#!/bin/sh
id=$1
if [ x"$id" = x ]; then
    printf "usage: $0 IDENTIFIER\n"
    exit 1
fi
basedir=$SCRATCH/mongodb/fireworks/$id
mkdir -p $basedir
mkdir $basedir/data
mongod --dbpath=$basedir/data --logpath=$basedir/mongo.log --fork
