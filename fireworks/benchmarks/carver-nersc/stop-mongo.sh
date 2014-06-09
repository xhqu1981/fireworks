#!/bin/sh
ps auxww | grep '[m]ongod' | cut -c10-16 | xargs kill
