use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME                => 'MColPro',
    AUTHOR              => q{WJIAN <cloveperl@gmail.com>},
    VERSION_FROM        => 'lib/MColPro.pm',
    ABSTRACT_FROM       => 'lib/MColPro.pm',
    PREFIX              => '/opt/MColPro/',
    LIB                 => '$(PREFIX)/lib',
    EXE_FILES           => [ qw( bin/exclude bin/initdb bin/mcolpro ) ],
    ($ExtUtils::MakeMaker::VERSION >= 6.3002
      ? ('LICENSE'=> 'perl')
      : ()),
    PL_FILES            => {},
    PREREQ_PM => {
        'YAML::XS'    =>  0.38,
        'Net::MySQL'  =>  0.11,
    },
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean               => { FILES => 'MColPro-*' },
);
