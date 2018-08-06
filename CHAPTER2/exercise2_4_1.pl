#!/usr/bin/perl

$end = 0;

$count = 10;

until ($end == 1) {

	print ($count,"\n");

	if ($count == 1) {

		$end = 1;

	}

	$count = $count - 1;

}
