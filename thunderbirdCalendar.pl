#!/usr/bin/env perl
use strict;
use POSIX;
use DBI;
use String::Escape qw(unbackslash);
use Switch;
use utf8;


my $cal_filename=$ARGV[0];
if (!$cal_filename){
  die "No input calendar file found";
}

my $home = $ENV{'HOME'};
my $thunderbird_dir = $home . "/.thunderbird";
my $ini_file = $thunderbird_dir . "/profiles.ini";
print "Ini File \ " . $ini_file . "\n";
my %profile_hash = {};
my %default_profile;

#
# Step 1: Get the profile from the .ini file:
#
open (FD, "<$ini_file") or die "Cannot open $ini_file for reading";
while (<FD>){
  if ($_ =~ /^\[(.*)\]/){
    %profile_hash = {};
  } else {
    if ($_ =~ /^([^=]*)=(.*)/){
      $profile_hash{$1} = $2;
      if (($1 eq "Default") and ($2 == 1)){
        %default_profile = %profile_hash;
      }
    }
  }
}
if (!$default_profile{'Default'}){
  die "Default profile not found";
}
my $profile_path;

if ($default_profile{'IsRelative'}==1){
  $profile_path = $thunderbird_dir . "/$default_profile{'Path'}";
} else {
  $profile_path = $default_profile{'Path'};
}
close(FD);

#
# Step 2: Get the Cal Id. It is the value of preference
#         "calendar.list.sortOrder".
#
our $cal_id;
our $db_file;
our $dbh;
our @tables_with_item_id;

my $prefs=$profile_path . "/prefs.js";
open(FD, "<$prefs") or die "Cannot open preference file for reading";
while(<FD>){
  if ($_ =~ /^\s*user_pref\("calendar.list.sortOrder"\s*,\s*"(.*)"\s*\)\s*;/){
    $cal_id = $1;
    last;
  }
}
close(FD);
if (!$cal_id){
  die "No calendar id found";
}
print "Cal Id = $cal_id \n";
#
# Step 3: Create the calendar object
#
my $cal_object = {};
my $cur_pointer = $cal_object;

my $keep_str;
$/ = "\r\n";
open(FD, "<$cal_filename") or die "Cannot open $cal_filename for reading";
while(<FD>){
  chomp;
  my $firstchar=substr($_,0,1);
  if ($firstchar eq ' ' or $firstchar eq "\t"){
    $$keep_str .= substr($_,1);
    my $a = $$keep_str;
  } elsif ($_ =~ /^([^:]+)\:(.*)/){
    if ($1 eq "BEGIN"){
      if (%$cur_pointer){
        push(@{$cur_pointer->{children}},{}); 
        my $next_pointer = $cur_pointer->{children}[-1];
        $next_pointer->{parent} = $cur_pointer;
        $cur_pointer = $next_pointer;
      }
      @{$cur_pointer->{children}} = ();
    } elsif ($1 eq 'END'){
      if (!(%$cur_pointer)){
        print "Warning! Unmatched End!\n";
      } elsif ($2 ne ${$cur_pointer->{BEGIN}}[0]){
        print "END w/out BEGIN\n";
        $cur_pointer = $cur_pointer->{parent};
      } else {
        $cur_pointer = $cur_pointer->{parent}
      }
      next;
    }
    if (!$cur_pointer->{$1}){
      @{$cur_pointer->{$1}}=();
    }
    push(@{$cur_pointer->{$1}},$2);
    $keep_str=\${$cur_pointer->{$1}}[-1];
  } 
}

#
# Step 4: Database operations
#
our $method=${$cal_object->{METHOD}}[0];
print "Method=$method\n";

our $item_flags = {
  private=>1,
  has_attendees=>1<<1,
  has_properties=>1<<2,
  event_allday=>1<<3,
  has_recurrence=>1<<4,
  has_exceptions=>1<<5,
  has_attachments=>1<<6,
  has_relations=>1<<7,
  has_alarms=>1<<8,
  recurrence_id_allday=>1<<9
};

sub get_tables_with_item_id {
  my $sth = $dbh->prepare(qq(
    SELECT name
      FROM sqlite_master
     WHERE
           type='table'
  ));
  my @retValue;
  $sth->execute;
  while (my @row=$sth->fetchrow_array){
    my $sth1 = $dbh->prepare(qq(pragma table_info($row[0])));
    $sth1->execute;
    my $recurrence_id_found=0;
    my $item_id_found=0;
    while (my @column_info = $sth1->fetchrow_array){
      $item_id_found=1 if $column_info[1] eq 'item_id';
      $recurrence_id_found=1 if $column_info[1] eq 'recurrence_id';
    }
    push(@retValue, {name=>$row[0], has_recurrence_id=>$recurrence_id_found})
      if $item_id_found;
  } 
  return @retValue;
}

sub delete_all_by_item_id {
  my $cal_id = shift;
  my $item_id = shift;

  for (@tables_with_item_id){
    $dbh->prepare(qq(
       DELETE FROM $_->{name}
        WHERE cal_id=?
          AND item_id=?))->execute($cal_id, $item_id);
  }
}

sub delete_by_item_id {
  my $cal_id = shift;
  my $item_id = shift;
  my $recurrence = shift;

  for (@tables_with_item_id){
    next if $recurrence->{datetime} and not $_->{has_recurrence_id};
    my $query=qq(DELETE FROM $_->{name}  
                  WHERE cal_id = ?
                    AND item_id = ?
    );
    if ($_->{has_recurrence_id}){
      $query .= $recurrence->{datetime}?
                 qq(AND recurrence_id = ?) :
                 qq(AND recurrence_id is NULL);
    }
    my $sth=$dbh->prepare($query);
    if ($recurrence->{datetime}){
      $sth->execute($cal_id, $item_id, $recurrence->{datetime});
    } else {
      $sth->execute($cal_id, $item_id);
    }
  }
}

sub getTZID{
  my $key=shift;
  my $str=shift;
  my $tzidStrInd = length("$key;");
  $str=substr($str, $tzidStrInd);
  my $tzidEQ = 'TZID=';
  my $len = length($tzidEQ);
  if (length($str) < $len || substr($str,0,$len) ne $tzidEQ){
    print "No TZID found\n";
    return 'floating';
  } 
  print "TZID is " . substr($str,$len) . "\n";
  return substr($str,$len);
}


sub dateToInt {
  my $dateString=shift;
  my $timezone = shift;
  if ($dateString =~ /(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})(Z)?/){
    my $year = $1 - 1900;
    my $month = $2 - 1;
    my $day = $3;
    my $hour= $4;
    my $min = $5;
    my $sec = $6;
    my $z = $7;
    if ($z){
      $ENV{'TZ'}='UTC'
    } else {
      $ENV{'TZ'}=$timezone;
    }
    tzset();
    return (intTime=>mktime($sec,$min,$hour,$day,$month,$year) * 1e6, allDay=>0);
  } elsif ($dateString =~ /(\d{4})(\d{2})(\d{2})/) {
    my $year= $1 - 1900;
    my $month= $2 - 1;
    my $day = $3;
    return (intTime=>mktime(0,0,0,$day, $month, $year) * 1e6, allDay=>1);
  }
}

# This function re-folds a string if the value required is that
# of a column named icalString.
sub fold73 {
  my $instr = shift;
  my $retval='';
  return $instr if length($instr) <= 75;
  my $first_iteration=1;
  my $substrlen=74;
  while (length($instr) > $substrlen){
    $retval .= ' ' unless $first_iteration;
    my $substrlen = $first_iteration? 74:73;
    $first_iteration = undef;
    $retval .= substr($instr,0,$substrlen);
    $instr = substr($instr,$substrlen);
    $retval .= "\r\n" if (length($instr));
  }
  $retval .= ' ' . $instr if $instr;
  return $retval;
}

sub handle_alarms {
  my $child_list=shift;
  my @ret_value;
  for (@{$child_list}){
    if (${$_->{BEGIN}}[0] ne 'VALARM'){
      next;
    }
    my $alarm_str="BEGIN:VALARM\r\n";
    print "ALARM\n";
    my $cur=$_;
    for (keys(%{$cur})){
      switch ($_){
        case /(^ACTION)|^(REPEAT)|(^TRIGGER)|(^DESCRIPTION)/{
          $alarm_str .= $_ .  ':' . ${$cur->{$_}}[0] . "\r\n";
        }
      }
    }
    $alarm_str .= "END:VALARM\r\n";
    push(@ret_value, $alarm_str);
  }
  return @ret_value;
}

sub get_event_rec {
  my $main_part_type=shift;
  my $cal_id = shift;
  my $uid=shift;
  my $recurrence=shift;
  
  my $sth;
  my $rv;

  print "CAL ID: $cal_id, UID=$uid, time=$recurrence->{datetime}\n";
  switch ($main_part_type){
    case 'VEVENT'{
      if ($recurrence->{datetime}){
        $sth = $dbh->prepare(qq(
          SELECT *
          FROM cal_events
          WHERE 
            cal_id=?
          AND
            id=?
          AND
            recurrence_id = ?
          ORDER BY event_stamp desc
          LIMIT 1));
         $sth->execute($cal_id, $uid, $recurrence->{datetime});
      } else {
        $sth = $dbh->prepare(qq(
          SELECT *
          FROM cal_events
          WHERE
            cal_id=?
          AND
            id=?
          AND
            recurrence_id is NULL
          ORDER BY event_stamp desc
          LIMIT 1));

         $sth->execute($cal_id, $uid);
      }
    }
    case 'VTODO'{
      if ($recurrence->{datetime}){
        $sth = $dbh->prepare(qq(
           SELECT *
           FROM cal_todos
           WHERE
             cal_id=?
           AND
             id=?
           AND
             recurrence_id=?
          ORDER BY todo_stamp desc
          LIMIT 1));
        $sth->execute($cal_id, $uid, $recurrence->{datetime});
      } else {
        $sth = $dbh->prepare(qq(
           SELECT *
           FROM cal_todos
           WHERE
             cal_id=?
           AND
             id=?
           AND
             recurrence_id is NULL
          ORDER BY todo_stamp desc
          LIMIT 1));
        $sth->execute($cal_id, $uid);

      }
    }  
  }
  return $sth->fetchrow_hashref;
}

sub insert_attendees {
  my $cal_id=shift;
  my $uid=shift;
  my $recurrence=shift;
  my $attendees=shift;
  print "Attendees: $attendees \n";
  return 0 if not @$attendees;
  my $retvalue=0;
  print "rec keys: " . join(',',keys(%$recurrence)) . "\n";
  for (@$attendees){
    print "rec: $recurrence->{'TZID'}\n";
    print "+++ Attendee: $_\n";
    my $sth = $dbh->prepare(qq (
      INSERT INTO cal_attendees
      (item_id, recurrence_id, recurrence_id_tz, cal_id, icalString)
      VALUES(?,?,?,?,?)));
    my $rv = $sth->execute($uid,$recurrence->{datetime},$recurrence->{TZID},
                            $cal_id, $_);
    print "Insert RV: $rv\n";
    $retvalue=$item_flags->{has_attendees} unless $rv<0;
  }
  return $retvalue;
}

sub insert_recurrences {
  my $cal_id = shift;
  my $item_id = shift;
  my $rrules = shift;
  my $retValue=0;
  return 0 if (!@$rrules);
  my $sth = $dbh->prepare(qq(
    INSERT INTO cal_recurrence
      (cal_id, item_id, icalString)
    VALUES (?,?,?)));
  for (@$rrules){
    my $rv=$sth->execute($cal_id, $item_id, $_);
    $retValue |= $item_flags->{has_recurrence} unless $rv<0;
  }
  return $retValue;
}

sub insert_attachments {
  my $cal_id=shift;
  my $item_id=shift;
  my $recurrence=shift;
  my $attachments=shift;
  my $retValue=0;

  my $sth=$dbh->prepare(qq(
    INSERT INTO cal_attachments
    (item_id, cal_id, recurrence_id, recurrence_id_tz, icalString)
    VALUES (?,?,?,?,?);
  ));
  for (@$attachments){
    my $rv = $sth->execute($item_id, $cal_id, $recurrence->{datetime},
                        $recurrence->{TZID}, $_);
    $retValue |= $item_flags->{has_attachments} unless $rv<0;
  }
  return $retValue;
}

sub insert_properties{
  my $cal_id=shift;
  my $item_id=shift;
  my $recurrence=shift;
  my $properties=shift;
  my $retValue=0;
  
  my $sth=$dbh->prepare(qq(
    INSERT INTO cal_properties
    (item_id, key, value, recurrence_id, recurrence_id_tz, cal_id)
    VALUES (?,?,?,?,?,?);
  ));
  for (@$properties){
    my $rv = $sth->execute($item_id, $_->{key}, $_->{value}, 
                        $recurrence->{datetime}, $recurrence->{TZID}, $cal_id);
    $retValue = $item_flags->{has_properties} unless $rv<0;
  }
  return $retValue;
}

sub insert_relations{
  my $cal_id=shift;
  my $item_id=shift;
  my $recurrence=shift;
  my $relations=shift;
  my $retValue=0;
 
  my $sth=$dbh->prepare(qq(
    INSERT INTO cal_relations
    (cal_id, item_id, recurrence_id, recurrence_id_tz, icalString)
    VALUES (?,?,?,?,?)
  ));
  for (@$relations){
    my $rv = $sth->execute($cal_id. $item_id, $recurrence->{datetime}, 
                        $recurrence->{TZID}, $_);
    $retValue = $item_flags->{has_relations} unless $rv<0;
  }
  return $retValue;
}

sub insert_alarms{
  my $cal_id=shift;
  my $item_id=shift;
  my $recurrence=shift;
  my $alarms=shift;
  my $retValue=0;

  my $sth=$dbh->prepare(qq(
    INSERT INTO cal_alarms
    (cal_id, item_id, recurrence_id, recurrence_id_tz, icalString)
    VALUES (?,?,?,?,?)
  ));
  for (@$alarms){
    print "Alarm: $_ \n";
    my $rv = $sth->execute($cal_id, $item_id, $recurrence->{datetime},
                        $recurrence->{TZID}, $_);
    $retValue = $item_flags->{has_alarms} unless $rv<0;
  }
  return $retValue;
}

sub delete_event_or_todo {
  my $table_name=shift;
  my $cal_id = shift;
  my $item_id = shift;

  $dbh->prepare(qq(
     DELETE FROM $table_name
      WHERE
            cal_id = ?
        AND
            id = ?
  ))->execute($cal_id, $item_id);
}

sub delete_exception_entry{
  my $table_name = shift;
  my $cal_id = shift;
  my $item_id = shift;
  my $recurrence = shift;

  my $sth = $dbh->prepare(qq(
     DELETE FROM $table_name
      WHERE
        cal_id = ?
      AND
        id = ?
      AND
        recurrence_id = ? 
  ));
  $sth->execute($cal_id, $item_id, $recurrence->{datetime});
}

sub reset_exceptions {
  my $table_name=shift;
  my $cal_id=shift;
  my $item_id=shift;

  my $sth = $dbh->prepare(qq(
     UPDATE $table_name
        SET flags = flags & ~$item_flags->{has_exceptions}
      WHERE
         cal_id=?
        AND
         id=?
        AND
         recurrence_id is NULL
     ));
  $sth->execute($cal_id, $item_id);
}

sub update_exceptions {
  my $table_name=shift;
  my $cal_id=shift;
  my $item_id = shift;

  my $sth = $dbh->prepare(qq(
     UPDATE $table_name
        SET flags = flags | $item_flags->{has_exceptions}
     WHERE
        cal_id=?
       AND
        id=?
       AND
        recurrence_id is NULL;
  ));
  $sth->execute($cal_id, $item_id);
}

sub count_exceptions {
  my $table_name=shift;
  my $cal_id=shift;
  my $item_id=shift;

  my $query = qq(
    SELECT count(*)
    FROM $table_name
    WHERE
          cal_id=?
      AND
          id = ?
      AND
          recurrence_id is not NULL
  );
  my $sth = $dbh->prepare($query);
  $sth->execute($cal_id, $item_id);
  my @row=$sth->fetchrow_array;
  return $row[0];
}

sub insert_todo {
  my $cal_id = shift;
  my $item_id = shift;
  my $recurrence = shift;
  my $todo_rec = shift;
  my $flags = shift;

  my $query = qq(
    INSERT INTO cal_todos(
      cal_id,
      id,
      time_created,
      last_modified,
      title,
      priority,
      privacy,
      ical_status,
      flags,
      todo_entry,
      todo_due,
      todo_completed,
      todo_complete,
      todo_entry_tz,
      todo_due_tz,
      todo_completed_tz,
      recurrence_id,
      recurrence_id_tz,
      alarm_last_ack,
      todo_stamp 
    )
    values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
  );
  $flags |= $item_flags->{has_exceptions} if 
            not $recurrence->{datetime} 
            and count_exceptions('cal_todos', $cal_id, $item_id)>0;
  $flags |= $item_flags->{event_allday} if 
            ($todo_rec->{DTSTART}->{datehash}->{allDay});
  $dbh->prepare($query)->execute(
      $cal_id,
      $item_id,
      $todo_rec->{'CREATED'}->{datehash}->{intTime},
      $todo_rec->{'LAST-MODIFIED'}->{datehash}->{intTime},
      $todo_rec->{'SUMMARY'},
      $todo_rec->{'PRIORITY'},
      $todo_rec->{'CLASS'},
      $todo_rec->{'STATUS'},
      $flags,
      $todo_rec->{'DTSTART'}->{datehash}->{intTime},
      $todo_rec->{'DUE'}->{datehash}->{intTime},
      $todo_rec->{'COMPLETED'}->{datehash}->{intTime},
      $todo_rec->{'PERCENT-COMPLETE'},
      $todo_rec->{'DTSTART'}->{tzid},
      $todo_rec->{'DUE'}->{tzid},
      $todo_rec->{'COMPLETED'}->{tzid},
      $recurrence->{datetime},
      $recurrence->{TZID},
      $todo_rec->{'X-MOZ-LASTACK'}->{datehash}->{intTime},
      $todo_rec->{'DTSTAMP'}->{datehash}->{intTime}
  );
  update_exceptions('cal_todos', $cal_id, $item_id) if $recurrence->{datetime};
}

sub update_todo {
  my $cal_id = shift;
  my $item_id = shift;
  my $recurrence = shift;
  my $todo_rec = shift;
  my $flags = shift;
  my $query = qq(
    UPDATE cal_todos
       SET
           time_created=?,
           last_modified = ?,
           title = ?,
           priority = ?,
           privacy = ?,
           ical_status = ?,
           flags = ?,
           todo_entry = ?,
           todo_due = ?,
           todo_completed = ?,
           todo_complete  = ?,
           todo_entry_tz = ?,
           todo_due_tz = ?,
           todo_completed_tz = ?,
           todo_stamp = ?
        WHERE
           cal_id = ?
        AND
          id = ?
  );
  if ($recurrence->{datetime}){
    $query .= qq(
         AND
           recurrence_id = ?
    );
  } else {
    $query .= qq(
         AND
           recurrence_id is NULL
    );
  }
  $flags |= $item_flags->event_allday if 
              ($todo_rec->{DTSTART}->{datehash}->{allDay});
   if ($recurrence->{datetime}){
     $dbh->prepare($query)->execute(
       $todo_rec->{'CREATED'}->{datehash}->{intTime},
       $todo_rec->{'LAST-MODIFIED'}->{datehash}->{intTime},
       $todo_rec->{'SUMMARY'},
       $todo_rec->{'PRIORITY'},
       $todo_rec->{'CLASS'},
       $todo_rec->{'STATUS'},
       $flags,
       $todo_rec->{'DTSTART'}->{datehash}->{intTime},
       $todo_rec->{'DUE'}->{datehash}->{intTime},
       $todo_rec->{'COMPLETED'}->{datehash}->{intTime}, 
       $todo_rec->{'PERCENT-COMPLETE'},
       $todo_rec->{'DTSTART'}->{tzid},
       $todo_rec->{'DUE'}->{tzid},
       $todo_rec->{'COMPLETED'}->{tzid},
       $todo_rec->{'DTSTAMP'}->{datehash}->{intTime},
       $cal_id,
       $item_id,
       $recurrence->{datetime}
   );
  } else {
   $flags |= $item_flags->{has_exceptions} if
       count_exceptions('cal_todos', $cal_id, $item_id)>0;
   $dbh->prepare($query)->execute(
       $todo_rec->{'CREATED'}->{datehash}->{intTime},
       $todo_rec->{'LAST-MODIFIED'}->{datehash}->{intTime},
       $todo_rec->{'SUMMARY'},
       $todo_rec->{'PRIORITY'},
       $todo_rec->{'CLASS'},
       $todo_rec->{'STATUS'},
       $flags,
       $todo_rec->{'DTSTART'}->{datehash}->{intTime},
       $todo_rec->{'DUE'}->{datehash}->{intTime},
       $todo_rec->{'COMPLETED'}->{datehash}->{intTime},
       $todo_rec->{'PERCENT-COMPLETE'},
       $todo_rec->{'DTSTART'}->{tzid},
       $todo_rec->{'DUE'}->{tzid},
       $todo_rec->{'COMPLETED'}->{tzid},
       $todo_rec->{'DTSTAMP'}->{datehash}->{intTime},
       $cal_id,
       $item_id
     );
  }
}

sub insert_event {
  my $cal_id = shift;
  my $item_id = shift;
  my $recurrence = shift;
  my $event_rec = shift;
  my $flags = shift;

  $flags |= $item_flags->{has_exceptions} if
            not $recurrence->{datetime} 
            and count_exceptions('cal_events', $cal_id, $item_id)>0;
  $flags |= $item_flags->{event_allday} if
           ($event_rec->{DTSTART}->{datehash}->{allDay});
  print "Insert Event\n";
 
  my $sth = $dbh->prepare(qq(
     INSERT into cal_events (
       cal_id,
       id,
       time_created,
       last_modified,
       title,
       priority,
       privacy,
       ical_status,
       flags,
       event_start,
       event_end,
       event_stamp,
       event_start_tz,
       event_end_tz,
       recurrence_id,
       recurrence_id_tz,
       alarm_last_ack
     )
     VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
  )); 
  my $tmphash = $event_rec->{DTSTART}; 
  print "Event date hash keys:" . join(',',keys(%$tmphash)) . "\n";
  $sth->execute(
     $cal_id,
     $item_id,
     $event_rec->{'CREATED'}->{datehash}->{intTime},
     $event_rec->{'LAST-MODIFIED'}->{datehash}->{intTime},
     $event_rec->{'SUMMARY'},
     $event_rec->{'PRIORITY'},
     $event_rec->{'CLASS'},
     $event_rec->{'STATUS'},
     $flags,
     $event_rec->{'DTSTART'}->{datehash}->{intTime},
     $event_rec->{'DTEND'}->{datehash}->{intTime},
     $event_rec->{'DTSTAMP'}->{datehash}->{intTime},
     $event_rec->{'DTSTART'}->{tzid},
     $event_rec->{'DTEND'}->{tzid},
     $recurrence->{datetime},
     $recurrence->{TZID},
     $event_rec->{'X-MOZ-LASTACK'}->{datehash}->{intTime}
  );
  update_exceptions('cal_events', $cal_id, $item_id) if $recurrence->{datetime};
}

sub update_event {
  my $cal_id=shift;
  my $item_id = shift;
  my $recurrence = shift;
  my $event_rec = shift;
  my $flags = shift;

  print "Update Event\n";
  my $query = qq(
    UPDATE cal_events
       SET
           time_created = ?,
           last_modified = ?,
           title = ?,
           priority = ?,
           privacy = ?,
           ical_status = ?,
           flags = ?,
           event_start = ?,
           event_end = ?,
           event_stamp = ?,
           event_start_tz = ?,
           event_end_tz = ?
       WHERE
           cal_id = ?
         AND
           id = ?
           );
  if ($recurrence->{datetime}){
    $query .= qq(
         AND
           recurrence_id = ?
    );
  } else {
    $query .= qq(
         AND
           recurrence_id is NULL
    );
  }
  $flags |= $item_flags->{event_allday} if ($event_rec->{DTSTART}->{datehash}->{allDay});

  if ($recurrence->{datetime}){
    $dbh->prepare($query)->execute(
       $event_rec->{'CREATED'}->{datehash}->{intTime},
       $event_rec->{'LAST-MODIFIED'}->{datehash}->{intTime},
       $event_rec->{'SUMMARY'},
       $event_rec->{'PRIORITY'},
       $event_rec->{'CLASS'},
       $event_rec->{'STATUS'},
       $flags,
       $event_rec->{'DTSTART'}->{datehash}->{intTime},
       $event_rec->{'DTEND'}->{datehash}->{intTime},
       $event_rec->{'DTSTAMP'}->{datehash}->{intTime},
       $event_rec->{'DTSTART'}->{tzid},
       $event_rec->{'DTEND'}->{tzid},
       $cal_id,
       $item_id,
       $recurrence->{datetime}
    );
  } else {
    $flags |= $item_flags->{has_exceptions} if
       count_exceptions('cal_events', $cal_id, $item_id)>0;
    $dbh->prepare($query)->execute(
       $event_rec->{'CREATED'}->{datehash}->{intTime},
       $event_rec->{'LAST-MODIFIED'}->{datehash}->{intTime},
       $event_rec->{'SUMMARY'},
       $event_rec->{'PRIORITY'},
       $event_rec->{'CLASS'},
       $event_rec->{'STATUS'},
       $flags,
       $event_rec->{'DTSTART'}->{datehash}->{intTime},
       $event_rec->{'DTEND'}->{datehash}->{intTime},
       $event_rec->{'DTSTAMP'}->{datehash}->{intTime},
       $event_rec->{'DTSTART'}->{tzid},
       $event_rec->{'DTEND'}->{tzid},
       $cal_id,
       $item_id
    );

  }
}

sub write_event {
  my $cal_id = shift;
  my $item_id = shift;
  my $recurrence = shift;
  my $event_rec = shift;
  my $flags = shift; 
  my $action = shift;
  print "Action = $action\n";
  if ($action eq 'U'){
    update_event ($cal_id, $item_id, $recurrence, $event_rec, $flags);
  } else {
    insert_event ($cal_id, $item_id, $recurrence, $event_rec, $flags);
  }
}

sub write_todo {
  my $cal_id = shift;
  my $item_id = shift;
  my $recurrence = shift;
  my $todo_rec = shift;
  my $flags = shift;
  my $action = shift;
  if ($action eq 'U'){
    update_todo ($cal_id, $item_id, $recurrence, $todo_rec, $flags);
  } else {
    insert_todo ($cal_id, $item_id, $recurrence, $todo_rec, $flags);
  }
}

sub add_exdate {
  my $tname=shift;
  my $cal_id=shift;
  my $item_id=shift;
  my $recurrence=shift;

  my $exdate_str='EXDATE:';
  my @time_arr=gmtime($recurrence->{datetime} / 1e6);
  my $sec=$time_arr[0];
  my $min=$time_arr[1];
  my $hr=$time_arr[2];
  my $dd=$time_arr[3];
  my $mm=$time_arr[4]+1;
  my $yyyy=$time_arr[5]+1900;
  $exdate_str .= $yyyy;
  $exdate_str .= $mm<10 ? '0' . $mm : $mm;
  $exdate_str .= $dd<10 ? '0' . $dd : $dd;
  if (not $recurrence->{allDay}){
    $exdate_str .= 'T';
    $exdate_str .= $hr<10 ? '0' . $hr : $hr;
    $exdate_str .= $min<10 ? '0' . $min : $min;
    $exdate_str .= $sec < 10 ? '0' . $sec: $sec;
    $exdate_str .= 'Z';
  }
  $exdate_str .= "\r\n";
  $dbh->prepare(qq(
        INSERT INTO cal_recurrence(item_id, cal_id, icalString)
        VALUES (?,?,?)))->execute($item_id, $cal_id, $exdate_str);

  $dbh->prepare(qq(
        UPDATE $tname
           SET flags = flags | $item_flags->{has_recurrence}
           WHERE id=?
             AND cal_id=?
             AND recurrence_id is NULL))->execute($item_id, $cal_id);
}
sub handle_main_part {
  print "Welcome to handle_main_part, cal_id: $cal_id\n";
  my $cur = shift;
  my $method=shift;
  my @attendees;
  my %recurrence;
  my @rrules;
  my @attachments;
  my @relations;
  my @alarms;
  my %main_part_rec;
  my @properties;
  my $main_part_type;
  
  print "Cur: " . join(',',keys(%$cur)) . "\n";
  for (keys(%$cur)){
    my $key;
    switch($_){
      case 'BEGIN' {
        $main_part_type = ${$cur->{$_}}[0];
      }
      case /^RECURRENCE-ID/ {
        $recurrence{'TZID'}=getTZID('RECURRENCE-ID',$_);
        my %dateTime=dateToInt(${$cur->{$_}}[0],$recurrence{'TZID'});
        $recurrence{'datetime'}=$dateTime{intTime};
        $recurrence{'allDay'}=$dateTime{allDay};
      }
      case /(^ATTENDEE)|(^ORGANIZER)/ {
        $key=$_;
        for (@{$cur->{$key}}){
          push(@attendees, fold73($key . ':'.$_)."\r\n");
        }
      }
      case ['RDATE','RRULE','EXDATE','EXRULE'] {
        $key=$_;
        for(@{$cur->{$key}}){
          push(@rrules, fold73($key . ':' . $_)."\r\n");
        }
      }
      case 'children'{
        @alarms=handle_alarms($cur->{$_});
      }
      case 'RELATED_TO' {
        $key=$_;
        for (@{$cur->{$key}}){
          push(@relations, fold73($key . ':' . $_)."\r\n");
        }
      }
      case /^ATTACH;?/ {
        $key=$_;
        for (@{$cur->{$key}}){
          push(@attachments, fold73($key . ':' . $_)."\r\n");
        }
      }
      # Fields of the event/todo table, which are not dates
      case ['UID', 'SUMMARY','PRIORITY','CLASS','STATUS','PERCENT-COMPLETE']{
        $main_part_rec{$_} = unbackslash(${$cur->{$_}}[0]);
      }
      # Dates of event/todo table
      case /^(DTSTART)|(CREATED)|(LAST-MODIFIED)|(DTSTAMP)|(DUE)|(DTEND)|(X-MOZ-LASTACK)|(COMPLETED)/{
        my $semicolonPos=index($_,';');
        my $key;
        if ($semicolonPos >= 0){
          $key = substr($_,0,$semicolonPos);
        } else {
          $key = $_;
        }
        my $tzid;
        $tzid=getTZID($key,$_) if $semicolonPos >= 0;
        my %datepart =  dateToInt(${$cur->{$_}}[0], $tzid);
        my $tmp = {datehash=>\%datepart, tzid=>$tzid};
        $main_part_rec{$key} = $tmp;
      }
      case 'CATEGORIES' {
        print "Categories: " . join(',', @{$cur->{$_}}) . "\n";
        my $tmp={key=>$_, value=>join(',', @{$cur->{$_}})};
        push(@properties,  $tmp);
      }
      else {
        next if ($_ eq 'children' or $_ eq 'parent');
        my $tmp={key=>$_, value=>unbackslash(${$cur->{$_}}[0])};
        push(@properties, $tmp);
      }
    }
  } 
  if (not $main_part_rec{UID}){
    print "Error: No ID found for event/todo\n";
    return;
  } 
  # Begin Transaction 
  my $sth=$dbh->begin_work;
  my $flags=0;
  if (not($method) || $method eq "REQUEST" || $method eq "PUBLISH"){
    my $event_rec = get_event_rec($main_part_type,$cal_id, $main_part_rec{UID}, \%recurrence); 
    my $main_part_stamp = $main_part_rec{DTSTAMP}->{datehash}->{intTime} . "\n";
    my $action;
    if (!$event_rec){
      $action='I';
      print "No rows returned: Insert\n";
    } else {
      my $stamp_in_db = $main_part_type eq 'VEVENT'? 
         $event_rec->{event_stamp}:$event_rec->{todo_stamp};
         print ("$main_part_type: Stamp in DB: $stamp_in_db\n");
         print "Main_part stamp: $main_part_stamp \n";
      if ($main_part_stamp > $stamp_in_db){
        $action='U';
        print "Update\n";
      } else {
        $dbh->rollback;
        return;
      }
    }
    # Update tables
    delete_by_item_id($cal_id, $main_part_rec{UID}, \%recurrence) if 
      $action eq 'U';
    $flags |= insert_attendees($cal_id, $main_part_rec{UID},
                               \%recurrence,\@attendees);
    $flags |= insert_recurrences($cal_id, $main_part_rec{UID}, \@rrules); 
    $flags |= insert_attachments($cal_id, $main_part_rec{UID}, \%recurrence, 
                                 \@attachments);
    $flags |= insert_properties($cal_id, $main_part_rec{UID}, \%recurrence, 
                                \@properties); 
    $flags |= insert_relations($cal_id, $main_part_rec{UID}, \%recurrence, 
                               \@relations);
    $flags |= insert_alarms($cal_id, $main_part_rec{UID}, \%recurrence,
                            \@alarms);
    if ($main_part_type eq 'VEVENT'){
      write_event($cal_id, $main_part_rec{UID}, \%recurrence, \%main_part_rec, 
                  $flags, $action);
    } else {
      write_todo($cal_id, $main_part_rec{UID}, \%recurrence, \%main_part_rec,
                 $flags, $action);
    }
  } elsif ($method eq "CANCEL") {
    my $tname = $main_part_type eq 'VEVENT'? 'cal_events':'cal_todos';
    if ($recurrence{datetime}){
      delete_by_item_id($cal_id, $main_part_rec{UID}, \%recurrence);
      add_exdate($tname, $cal_id, $main_part_rec{UID}, \%recurrence);
      delete_exception_entry($tname, $cal_id, $main_part_rec{UID}, \%recurrence);
      reset_exceptions($tname, $cal_id, $main_part_rec{UID}) if 
         count_exceptions($tname, $cal_id, $main_part_rec{UID})==0;
    } else {
      delete_event_or_todo($tname, $cal_id, $main_part_rec{UID});
      delete_all_by_item_id($cal_id, $main_part_rec{UID});
    }
  }
  $dbh->commit;
}

#
# Scanning the calendar object tree
#
$db_file = $profile_path . "/calendar-data/local.sqlite";
print "db_file = $db_file\n";
my $driver = 'SQLite';
my $dsn = "DBI:$driver:dbname=$db_file";
my $username="";
my $password="";
$dbh=DBI->connect($dsn, $username, $password, {RaiseError=>1})
  or die $DBI::errstr;

# A list of tables having a field named 'item_id'. 
@tables_with_item_id = get_tables_with_item_id;

my @nodes = ( $cal_object );
print "DEBUG:  $cal_object->{children}\n";
for (@{$cal_object->{children}}){
  if (${$_->{BEGIN}}[0] eq 'VEVENT' or ${$_->{BEGIN}}[0] eq 'VTODO'){
    handle_main_part($_,$method);
  }
}
