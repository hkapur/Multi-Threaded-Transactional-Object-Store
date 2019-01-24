#!/bin/bash

rm -f tests/xacto_tests.c
cp -p lib/xacto_nodebug.s lib/xacto.a
make clean setup bin/xacto
make -f Makefile.test include/excludes.h
make bin/xacto_tests
