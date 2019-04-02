#!/usr/bin/env python3
import argparse
import array
import math


def is_prime(number):
    """
    Determine whether a number is prime.

    Args:
        number (int): The number to check

    Returns:
        (bool): True if the number is prime, false otherwise.
    """
    # 1 is not prime
    if number == 1:
        return False
    # 2 and 3 are prime
    elif number < 4:
        return True

    # Even numbers, except 2, are not prime
    if number % 2 == 0:
        return False

    # Check remaining, odd numbers up to the square root of number
    trials = (i for i in range(3, int(math.sqrt(number))+1, 2))
    for trial in trials:
        if number % trial == 0:
            return False
    return True


def adjust_bound(bound, upper_or_lower):
    if upper_or_lower == 'upper':
        shift = -1
    elif upper_or_lower == 'lower':
        shift = 1

    if bound % 2 == 0:
        return bound + shift
    else:
        return bound


if __name__ == '__main__':
    # Command line arguments
    parser = argparse.ArgumentParser(
        description='Find prime numbers within a range'
        )
    parser.add_argument('lower', type=int,
                        help='Lower bound of the range to search')
    parser.add_argument('upper', type=int,
                        help='Upper bound of the range to search')
    args = parser.parse_args()

    # Ensure lower bound is a positive integer
    if args.lower < 1:
        raise ValueError('Lower bound must be positive')

    # 4 byte integer array
    found = array.array('l')

    # Add 2, the exception, being an even prime
    if args.lower <= 2:
        found.append(2)

    # Don't try even numbers
    lower = adjust_bound(args.lower, 'lower')
    upper = adjust_bound(args.upper, 'upper')

    for test in range(lower, upper+1, 2):
        if is_prime(test):
            found.append(test)

    # print(found.tolist())
    for prime in found:
        print(prime)
