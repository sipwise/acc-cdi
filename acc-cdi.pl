#!/usr/bin/perl -w
# vim set sw=4 ts=4 et noai
#----------------------------------------------------------------------
# Sipwise Development Team <support@sipwise.com> 
#----------------------------------------------------------------------
## no critic
use strict;
our $VERSION = 1.0;
use DBI;
use Time::HiRes qw(gettimeofday);
use Date::Format qw(time2str);
use Data::Dumper;
#----------------------------------------------------------------------
BEGIN {
    $SIG{TERM}     = \&release_term;
    $SIG{INT}      = \&release_int;
    $SIG{__DIE__}  = \&ERROR;
}
my %CONFIG             = ();
my $pid_file           = '/var/run/ngcp-acc-cdi.pid';
my $config_file        = '/etc/ngcp-acc-cdi/acc-cdi.conf';
my $mysql_sipwise_conf = '/etc/mysql/sipwise.cnf';
my $dbh;
my $ch  = {};
my $dsn = 'DBI:mysql:database=accounting;host=localhost';
my %cnt = qw(updated 0 cleaned 0 finished 0 skipped 0);

# Read config
#----------------------------------------------------------------------
sub read_config {
    open (my $config_fh, $config_file) 
        || die "Can't open config file: $!";
    while (<$config_fh>) {
        chomp $_;
        $_ =~ /^\s*(\S*)\s*=\s*(.+)$/;
        next unless $1 && $2;
        $CONFIG{$1} = $2;
    }
    close $config_fh;

    # use sipwise user to access mysql
    open (my $mysql_fh, $mysql_sipwise_conf) 
        || die "Can't open mysql credentials file: $!";
    my $mysql_pass = <$mysql_fh>;
    chomp $mysql_pass;
    $mysql_pass =~ s/^SIPWISE_DB_PASSWORD='(.+)'.*$/$1/;
    $CONFIG{MYSQL_PASS} = $mysql_pass;
    close $mysql_fh;

    # redirect out/err to the log file
    if ($CONFIG{LOGGING}) {
        open (STDOUT, ">>" . $CONFIG{LOG_FILE}) 
            || die "Can't open logfile: $!";
        open (STDERR, ">>" . $CONFIG{LOG_FILE}) 
            || die "Can't open logfile: $!";
    }
}

# Logging 
#----------------------------------------------------------------------
sub INFO {
    my $msg = shift;
    return unless $msg;
    chomp $msg;
    my $time_s = '[' . time2str('%Y-%m-%d %T', time).']';
    print $time_s .' '.$msg."\n";
}

sub DEBUG {
    return unless $CONFIG{DEBUG};
    INFO shift;
}

sub ERROR {
    INFO shift;
    $dbh->rollback if $dbh;
    $dbh->disconnect if $dbh;
    exit 1;
}

# Init/release
#----------------------------------------------------------------------
sub init {
    read_config();
    # check/create lock
    if (-f $pid_file) {
        open(my $pf_fh, $pid_file) || die "Can't open file: $!";
        my $old_pid = <$pf_fh> || 0;
        chomp $old_pid;
        if ($old_pid && kill 0, $old_pid) {
            INFO "Already running.";
            exit 0;
        } else {
            DEBUG "Releasing stale lock $old_pid";
            unlink $pid_file || DEBUG "Can't release old lock: $!";
        }
    }
    open(my $pf_fh, '>'.$pid_file) || die "Can't create lock: $!";
    print $pf_fh $$."\n";
    INFO "Started.";
}

sub release_int {
    DEBUG "-- Interrupted --";
    release();
    exit 1;
}

sub release_term {
    DEBUG "-- Terminated --";
    release();
    exit 1;
}

sub release {
    $dbh->disconnect if $dbh;
    INFO sprintf(<<MSG, @{\%cnt}{qw(updated cleaned finished skipped)});
Done, marks updated %d cleaned %d, acc records finished %d stale %d
MSG
    DEBUG "Releasing lock $$";
    unlink $pid_file || warn "Can't release lock: $!";
}

# Check if the node is active
#----------------------------------------------------------------------
sub check_active {
    
    open (my $active, $CONFIG{NGCP_CHK_ACT}." |")
        || die "Can't run $CONFIG{NGCP_CHK_ACT} : $!";

    my $rc = <$active>;
    chomp($rc);

    return $rc eq '!' ? 1 : 0;
}

# Get active calls from kamailio
#----------------------------------------------------------------------
sub get_active_calls {
    open(my $calls, $CONFIG{NGCP_KAMCTL}." proxy fifo dlg_list |")
        || die "Can't open kamctl: $!";

    my @active_calls;;
    while (<$calls>) {
        chomp $_;
        if ($_ =~ /callid::\s+(\S+)$/) {
            push @active_calls, $1;
        }
    }
    close $calls; 
    return \@active_calls;
}

# Get start acc records (single INVITES with sip_reason = OK)
#----------------------------------------------------------------------
sub get_start_acc {

    my $ch = $dbh->prepare(<<SQL)
SELECT a.method, a.from_tag, a.to_tag, a.callid, 
       a.sip_code, a.sip_reason,
       a.time, a.time_hires, 
       a.dst_ouser, a.dst_domain, a.src_domain,
       c.mark
  FROM kamailio.acc a
LEFT JOIN acc_cdi c ON c.callid = a.callid
 WHERE a.method = 'INVITE' 
   AND a.sip_reason = 'OK'
   AND a.callid NOT IN 
    (SELECT b.callid 
       FROM kamailio.acc b 
      WHERE a.callid = b.callid 
        AND b.method != 'INVITE')
SQL
        || die "Can't prepare query: ".$DBI::errstr;
    
    my $rc = $ch->execute() || die "Can't execute query: ".$DBI::errstr;

    INFO sprintf("Fetched %d start acc records.", $rc);

    my %start_acc = ();
    while (my $row = $ch->fetchrow_hashref) {
        $start_acc{$row->{callid}} = $row;
    }

    $ch->finish();

    $dbh->commit;
    die "Can't commit transaction: ".$DBI::errstr if $DBI::err;

    return \%start_acc;
}

# Prepare sql statements that will be called repeatedly 
#----------------------------------------------------------------------
sub prepare_statements {

        $ch->{insert_mark} = $dbh->prepare(<<SQL)
INSERT INTO acc_cdi
(callid, mark)
VALUES
(?, ?)
SQL
            || die "Can't prepare query: ".$DBI::errstr;

        $ch->{update_mark} = $dbh->prepare(<<SQL)
UPDATE acc_cdi
   SET mark = ?
 WHERE callid = ?
SQL
            || die "Can't prepare query: ".$DBI::errstr;

        $ch->{delete_mark} = $dbh->prepare(<<SQL)
DELETE FROM acc_cdi
 WHERE callid = ?
SQL
            || die "Can't prepare query: ".$DBI::errstr;

        $ch->{finish_acc} = $dbh->prepare(<<SQL)
INSERT INTO kamailio.acc
(method, from_tag, to_tag, callid, 
 sip_code, sip_reason,
 time, time_hires, 
 src_leg, dst_leg,
 dst_ouser, dst_domain, src_domain)
VALUES
(?, ?, ?, ?, ?, '', '', ?, NOW(), ?, ?, ?, ?)
SQL
            || die "Can't prepare query: ".$DBI::errstr;
}

# Update marks in accounting.acc_cdi
#----------------------------------------------------------------------
sub update_mark {
    my $acc = shift;
    die "No acc passed into update_mark" unless ref $acc;

    eval {
        my $time_hires = scalar gettimeofday();
        my $duration = $time_hires - $acc->{time_hires};
        # already have the mark, update it
        if ($acc->{mark}) {
            DEBUG sprintf("Updating mark for callid: %s with duration: %d", 
                           $acc->{callid}, $duration);
            $ch->{update_mark}->execute($acc->{callid}, $time_hires)
                || die "Can't execute query: ".$DBI::errstr;
        # insert a new intermediate mark record
        } else {
            DEBUG sprintf("Inserting mark for callid: %s with duration: %d", 
                           $acc->{callid}, $duration);
            $ch->{insert_mark}->execute($acc->{callid}, $time_hires)
                || die "Can't execute query: ".$DBI::errstr;
        }
    };
    if ($@) {
        ERROR $@;
    }

    $dbh->commit;
    die "Can't commit transaction: ".$DBI::errstr if $DBI::err;
    $cnt{updated}++;
}   

# Finish acc records (create BYE for INVITES with marks but not active)
#----------------------------------------------------------------------
sub finish_acc {
    my $acc = shift;
    die "No acc passed into fisnih_acc" unless ref $acc;

    my $duration = $acc->{mark} - $acc->{time_hires};

    DEBUG sprintf("Finishing callid: %s with duration: %d", 
                  $acc->{callid}, $duration);

    eval {
        $acc->{method}     = 'BYE';
        $acc->{sip_reason} = 'Interrupted';
        $acc->{src_domain} = '127.0.0.1';

        # create the BYE (mediator will do the rest)
        $ch->{finish_acc}->execute(@{$acc}{qw(method from_tag to_tag 
                                              callid
                                              sip_code sip_reason
                                              mark
                                              dst_ouser dst_domain 
                                              src_domain)
                                       })
            || die "Can't execute query: ".$DBI::errstr;

        # cleanup the related mark
        $ch->{delete_mark}->execute($acc->{callid})
            || die "Can't execute query: ".$DBI::errstr;
    };
    if ($@) {
        ERROR $@;
    }

    $dbh->commit;
    die "Can't commit transaction: ".$DBI::errstr if $DBI::err;
    $cnt{finished}++;
}

# Clean up marks for already normally completed calls
#----------------------------------------------------------------------
sub cleanup_marks {

    eval {
    
        $cnt{cleaned} = int $dbh->do(<<SQL);
DELETE FROM acc_cdi
 WHERE callid NOT IN
       (SELECT callid 
          FROM kamailio.acc)
SQL
        die "Can't execute: ".$DBI::errstr if $DBI::err;

    };
    if ($@) {
        ERROR $@;
    }

    $dbh->commit;
    die "Can't commit transaction: ".$DBI::errstr if $DBI::err;

}
# Main (start) entry
#----------------------------------------------------------------------
sub main {

    # only run on the active node
    unless (check_active()) {
        DEBUG "The node is not active... stopping.";
        exit 0;
    }

    # db connect, with tx support for inserts/updates
    $dbh = DBI->connect($dsn, 'sipwise', $CONFIG{MYSQL_PASS}, 
                        { AutoCommit => 0 });
    die "Can't connect to mysql: ".$DBI::errstr if $DBI::err;

    # fetch start acc records, active calls for processing
    my $start_acc    = get_start_acc();
    my $active_calls = get_active_calls();

    prepare_statements();

    #print Data::Dumper->Dumpxs([$start_acc]),"\n";

    foreach my $callid (keys %$start_acc) {
        # active call
        if (grep /^$callid$/, @$active_calls) {
            update_mark($start_acc->{$callid});
        # inactive call but with a mark record
        } elsif ($start_acc->{$callid}{mark}) {
            finish_acc($start_acc->{$callid});
        # inactive call w/o mark record = stale
        } else {
            $cnt{skipped}++;
        }
    } 

    # records in acc_cdi w/o any related acc records in kamailio.acc 
    # =normally finished calls by kamailio/mediator
    cleanup_marks();
}
#----------------------------------------------------------------------

init();
main();
release();

exit 0;

# vim set sw=4 ts=4 et noai
__END__

NOTE: accounting.acc_cdi table is required for the script to operate properly.
      normally covered by ngcp db schema.

CREATE TABLE `acc_cdi` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `callid` varchar(255) NOT NULL,
  `mark` decimal(13,3) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `callid` (`callid`),
  KEY `callid_idx` (`callid`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8

