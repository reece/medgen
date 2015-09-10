#!/bin/sh

export NOSE_OPTS="--with-xunit --with-doctest --detailed-errors --xunit-file=tests/nosetests.xml" 

nosetests $NOSE_OPTS --with-coverage --cover-package medgen
