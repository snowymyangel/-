#!/usr/bin/perl
use strict;
use DBI;
my ${taskFile} = $ARGV[0];

my ${DSOURCE} = 'wh2800';

my $ret = main();
exit($ret);


my @jobs = ();
my @dependency = ();
my @lvl = ();


sub main
{
    my ${tasks} = readTasks();
    my $recursionSQL = "with 
recursive rectbl
(etl_system , etl_job , dependency_job , lvl )
as 
(
select etl_system , etl_job , dependency_job , 1(integer)
from petl.etl_job_dependency 
where etl_job  in ( ${tasks})
union all 
select   T.etl_system , T.etl_job , T.dependency_job , N.lvl+1
from 
rectbl N
inner join 
 petl.etl_job_dependency T
 on N.etl_job =  T.dependency_job 
  )
 sel distinct etl_job , dependency_job , lvl from rectbl
 where etl_job in (${tasks}) and dependency_job in (${tasks})
 order by lvl;";
 readRecursiveResult($recursionSQL);
 print "$recursionSQL\n\n\n";
    for(my $i = 0 ; $i<=$#jobs ; $i++)
    {
        print $jobs[$i]."\t".$dependency[$i]."\t".$lvl[$i]."\n";
    }
    return 0;
}

sub readRecursiveResult
{
    my $sql = $_[0];
    my @row = ();
    my $str = "";

    my $dbh = connectetl();
    my $sth = $dbh -> prepare( $sql );
    $sth -> execute();

    while( @row = $sth ->fetchrow_array )
    {
        unshift @jobs , $row[0];
        unshift @dependency , $row[1];
        unshift @lvl , $row[2];
    }
    $sth->finish();
    $dbh->disconnect();
    return 0;
}

sub readTasks
{
    my $tasks = '';
    my $str = '';
    open(RF , "<${taskFile}") or die "Can not open file ${taskFile}";
    $str = <RF>;
    $str=~s/[\x00-\x20]//g;
    $tasks = "'$str";
    while($str = <RF>)
    {
        $str=~s/[\x00-\x20]//g;
        $tasks = $tasks."',\n'".$str;
    }
    $tasks = $tasks."'";
    close(RF);
    return $tasks;
}


sub connectetl
{
    my $connectString = "dbi:ODBC:${DSOURCE}";   #此处原来DBI为小写：dbi 
    my ${dbh} = DBI->connect($connectString,"yangdapeng","wenyuan_xuhui");
    return $dbh;
}



