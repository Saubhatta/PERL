#!/usr/bin/perl

$done = 0;

$count = 1;

print ("This is printed before the loop starts.\n");

while ($done == 0) {

	print ("The valuse of count is ", $count, "\n");

	if ($count == 3) {

		$done = 1;
	}
	
	$count = $count + 1;

}

print ("End of loop.\n")
