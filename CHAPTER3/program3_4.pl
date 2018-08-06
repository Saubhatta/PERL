#!/usr/bin/perl

print ("Enter a line if input:\n");

$inputline = <STDIN>;

print ("uppercase; \U$inputline\E\n");

print ("lowercase : \L$inputline\E\n");

print ("as a sentence: \L\u$inputline\E\n");
