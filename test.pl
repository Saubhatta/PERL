#!/usr/bin/perl

$resultstring = "";

print("Enter your input - type an empty line to quit\n");

$input = <STDIN>;

chop ($input);

while ($input ne "") {

        $resultstring .= $input;

        $input = <STDIN>;

        chop ($input);

}

print ("Here is the final string:\n");

print ("$resultstring\n");
