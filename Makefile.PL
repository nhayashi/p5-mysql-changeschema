use inc::Module::Install;
name 'MySQL-ChangeSchema';
all_from 'lib/MySQL/ChangeSchema.pm';

# requires '';

tests 't/*.t';
author_tests 'xt';

test_requires 'Test::More';
auto_set_repository;
auto_include;
WriteAll;
