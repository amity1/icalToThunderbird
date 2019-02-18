# ICS To Thunderbird Calendar
This purpose of this Perl script is to add events and tasks published
on the Web to the Thuderbird calendar.
For example, from the link "Add to calendar" in the meetup site 
(https://meetup.com).

You can chhose from that link a calendar to which to add to Outlook,
or Yahoo Calendar or Google Calendar. In addition, you can choose "iCal" to
download it. If you download the link, you can save it or define a program
to run it.

This script will add events to the defualt user's calendar, assuming it is the 
only calendar.

## Requirement
This script is to be run on a UNIX like machine. It is written in Perl 5.26
and adds to calendars in Thunderbird 60.4.0.
### Modules:
Make sure you have the following modules installed and accessible by your
account. You can install them with *cpan*:
- DBI
- DBD::SQLite
- String::Escape

Using *cpan* is easy, for example:
```
cpan String::Escape
```

## Notes:
- **It is recommended to backup your default user's directory before running this script**

- If you don't see the result immediately, try restarting your Thunderbird.

- If you encounter any problem you can contact me via GitHub, or fix it
  yourself - It is an open source product.
