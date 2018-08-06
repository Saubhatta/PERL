#!/usr/bin/perl
print ("Enter a number:\n");
$number = <STDIN>;
chop ($number);
if ($number) {
	print ("The number is not zero.\n");
}

print ("This is the last line of the program.\n");
