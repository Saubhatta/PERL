#!/usr/bin/perl

print ("Enter first number:\n");

$number1 = <STDIN>;

chop ($number1);

print ("Enter second number:\n");

$number2 = <STDIN>;

chop ($number2);

if ($number2 == 0) {

	print ("Error: can't divide by zero\n");

} elsif ($number1 == 0) {

	print ("The number is ", $number1, ". because no division is necessary\n");

} elsif ($number2 == 1) {

	print ("The number is ", $number1, ". because no division is necessary\n");

} else {

	$result = $number1 / $number2; 		#Output

	print($number1, " divided by ", $number2, " equals ", $result, "\n");

}
