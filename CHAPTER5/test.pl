#!/usr/bin/perl
# Optima FTP Script
#
# Version: 3.27
# Date: 27/06/08
# Author: Matthew Selby, Naeem Ahmad, SGlass, ...
# Description: FTP script for VF Italy Nortel RNC UTRAN Files
#
# v2.0 - SGlass.  Use ini file, HASH for file match, dated list of files
# v2.1 - SGlass.  Log instead of die and cleanup before exit.
# v2.2 - SGlass.  Dated directories or dated filenames, downloadable files only in list, unzip parameters
# v2.3 - PWilson. Functionality replaced
# v2.4 - SGlass.  Heartbeating, spaces in ini files for values
# v2.5 - SGlass.  No date FTP, Environment variables in ini file
# v2.6 - CLedger. Functionality replaced
# v2.7 - SGlass.  sub-dir PREPEND, static string PREPEND (filenames)
# v3.0 - SGlass.
#    (3.0-1) Add variable for replace : in filename with '?'  ReplaceColonWith
#    (3.0-2) Allow FTPStyle to describe an array of \s+ seperated fields - must include size, file. Optionally include perm, and (date, time) or dateortime
#    (3.0-3) No spaces in filename
#    (3.0-4) Add variable for FTPDateFormat and FTPTimeFormat Use FTP date and FTPTimeOffset (in hours) and FTPSafetyPeriod (in mins) to filter out downloaded files.
#    (3.0-5) Change numberOfDays to be number of days after startOnDay rather than from 0
#    (3.0-6) Use Parameter for DatedDir number to determine depth DirDepth=DatedDir
#    (3.0-7) Use prepended filenames to determine 'collected' files.  (So prepend before determining if download required)
#    (3.0-8) Set FTPType=[ASCII||BINARY] to determine file download style (default=ASCII)
#    (3.0-9) INI Parameter for useMonitorFile (to be used for heartbeating or not)
# v3.1 - SGlass.
#    (3.1-1) Fix FIles downloaded count
#    (3.1-2) Allow a specified prepend string seperator.
# v3.2 - SGlass.
#    **(3.2-1) Load all Downloaded filenames into memory, Modify write to file process
#    **(3.2-2) Multiple RemoteDir processing.
#    *(3.2-3) Cleanup of single log file after n rows
#    *(3.2-4) FTPStyle to support stdUNIX and stdWINDOWS setup  (use date if FTPSafetyPeriod is set) (also set 'expected' Date and Time Format)
#    (3.2-5) excludeMask added to regexp the files NOT to download
# v3.3 - SGlass.
#    (3.3-1) New parameters for putting the files into sub-directories on 'local' using filename (tosubdir, fileDelim) default OFF
#    (3.3-2) Allow FTP to delete remote file (removeOnDownload) default OFF
#    (3.3-3) Set MAXFilesize to NOT download files greater than size in bytes
# v3.4 - SGlass.
#    (3.4-1) New parameters for 'untar' files (untarCommand)
#    *(3.4-2) FTP doesn't download 0 byte files
#    *(3.4-3) Output files in N output directories
#    (3.4-4) Handle SIG Interupts
#    (3.4-5) Debug level (Error message level)
# v3.5 - SGlass.
#    (3.5-1) Fix for heartbeat taking too long bug
#    (3.5-2) New Parameter PrependTimestamp (adds creation time to filename) requires DATE and TIME in FTPStyle
# v3.6 - SGlass.
#    (3.6-1) New Parameter removeZipExtBeforeMatch, takes away the zipExtention before checking if it needs to be downloaded.  handles archived files on remoteHost
#    (3.6-2) New Parameter remoteArchiveDir, moves the remoteFiles to ArchiveDir using 'fullFileName' in a flat directory on remoteHost using FTP.
# v3.7 - Naeem Ahmad.
#    New Parameter SFTP, to support secured FTP, if SFTP=0 then normal FTP else if SFTP=1 then secured FTP
# v3.8 - Naeem Ahmad.
#    fix bug#air00076208 : version number and program desciption is not displayed according to standard of OSS backend components.
#    fix bug#air00076733 : to make it backward compatible.
# v3.9 - Naeem Ahmad.
#    fix bug#air00076733 : FTP application should be backward compatible if SFTP is not installed.
#    use require instead of use for SFTP Module reference.
# v3.10 - Naeem Ahmad.
#    fix bug#00078736 : SFTP script does not work recursive on directories.
#    merge sebastian changes in version 3.6 to fix problem relate with PrependSubDir
#    Merge the recommendation made by Vodafone Italy to handle zip or tar file with space, to put file under double quotes.
# v3.11 - Naeem Ahmad.
#    fix bug#00079139 : FTP now support also another list of short hand month name, see @MTH2 array.
#    use dateFormat=-MTH4DD
# v3.12 - Naeem Ahmad.
#    if ftp not sftp get command fails, retry 1 more time ftp get after  reconnection to ftp server.
#    if  fails after retry, then ignore that file log the message for this file, grab next file.
#    Add new optional INI file parameter MAXFilesInDownloadDir, do not download any files if no output directories got files less than  MAXFilesInDownloadDir
#    If MAXFilesInDownloadDir not exist in INI file or MAXFilesInDownloadDir=0 then ignore the limit.
# v3.13 - Naeem Ahmad.
#    ftp safety period does not work when FTPSafetyPeriod > 1
# v3.14 - Naeem Ahmad.
#    Use import NET::SFTP along with  require NET::SFTP; to avoid the recompilation for each call for SFTP.
#    CQ : air00089009
# v3.15 - D Defilippi.
#     CQ : air00091759 - Added checks to find out if directories exit (28/03/2007)
# v3.16 - S Glass + D Defilippi.
#     CQ : air00091931 - Multiple parameters for IP Addresses - to be used sequentially (on FTP connect only - first connection wins!)
# v3.17 - D Defilippi
#   - air00084592 : Enchanced Message Reporting on File Download
#   - air00085063: Tidy up messages
#   - air00084590: Append options to be added to (S)FTP
#   - air00078228: FTP-GEN-010-GEN-302: Add PRID to list file name, temp file, etc...
#   - air00085217: SFTP functions are interpreted on each use
#   - air00086974: Log Severity level is 0-5 not the standard 1-6
#   - air00086973: fix bug#00079139 is not implemented correctly
#   - Reviewed all log messages ids and the last used is 40 therefore the next available to be used will be 41
# v3.18 - D Defilippi/Matthew Selby
#   - air00097567: Add an extra option like MINFileSize
#   - air00097604: Allow to download without looking for file size, if no SIZE in FTPStyle
#   - air00093953: When PID present, generate a critical log message
#   - air00097034: Correct message when file size is zero
#   - air00093954: "Backed up" log message is DEBUG not INFORMATION
# v3.19 - M Selby - air00097804: FTP-GEN-010-GEN-302: A .tmp file is left when dateFormat=0
# v3.19.01 BETA
#   - added dbgprint for (temporary) debug help
#   - fixed incorrect my $var1,$var2,... [needs my(...)]
#   - try to ignore lines of dir listing that are not files (eg <blank>, "Total of x files",...)
#     (check at least that we have enough fields compared to FTPStyle)
#   - test for year,month,day assigned, in correct range (when using FTPDateFormat)
#     (fix for timelocal() bombing with "Day '' out of range...")
# v3.19.02 BETA
#   - defaultFTPDirMatch is now "d........." not "drwx"
#   - fixed quoting of \s in
#   - totalkbytes can be formatted to int or 123.4 (default) or ...
#   - fixed refs to @MTH2 (was @local_MTH)
# v3.20 - re-fix for air00097804 (.tmp file left when dateFormat=0)
#       - commented dbgprint
#       - fix ini parsing to remove multiple hash-comments on line
# v3.21.01 BETA
#   - Ignore initial "Directory..." line (VMS)
#   - Tidy up ini settings with setIniOrDefault/setIniOrDie
#   - Added stdVMS options for FTPStyle
#   - air00097035: Parses VMS style directory listings, recognizes .DIR files, do VMS style directory-changing
#   - Added FTPActive option to ini file
# v3.22 - air00098566: Fix for problem with datedDir, incorrect listfile name
# v3.23 - air00098675: New v3.23 for version wrapped in .exe
# v3.24 - air00088350: Replace "rename" with "move" (from File::Copy) to allow for different partitions (eg for temp directory)
#       - Added "CreateExe.pl" script
# v3.25 - air00100716: Check that downloaded files are in WRITE permission
# v3.26 - options localFile, localFileList for local directory instead of ftp (enable nfs mount usage)
# v3.27 - New method for FTPSafetyPeriod based on recording time file first seen into safetyCheckList
# v6.1.0.0 - as 3.27, for 6.1 release
#   NB files on FTP site are assumed to have distinct names (not duplicated, even in
#   different directories). If not, need to feed directory into lastfiles.log and safetyCheckList
#
# v6.2.0.0 - as 3.27, for 6.2 dev/release. Next increment is 6.2.0.1 etc
# v6.2.0.1 - option encryptedPass to support encrypted passwords (requires script to be wrapped as .exe)
#           - tidied up logMessage numbers and die --> dieNicely (remove PID file)
# v6.2.0.2 - remove option encryptedPass (make it mandatory)
# v6.2.0.3 - For multiple FTP hosts, decrypt whole of RHS of remotepass=* (NOT individually, separated by commas)
#          - Build with latest crypter library.
#   NB files on FTP site are assumed to have distinct names (not duplicated, even in
#   different directories). If not, need to feed directory into lastfiles.log and safetyCheckList
#
# v6.2.0.3_b - SG Additional - Add ability to uncompress untar uncompress.
# v6.2.0.4 - Added functionality for the new DFL (Directory File Limit) functionaltiy to be part of Optima Backend
# v6.2.0.5 - amended DFL functioality for full backward compatibiliy
# v6.2.0.6 - Amended DFL default value to be 0 and not 10k as originally required.
# v6.2.0.7 - Amended DFL with the new extra requirements for AT&T.
# v6.2.0.8 - Added UseFolderFileLimit to be consistence with the BAFC changes
# v6.2.0.9 - Folder Limit sub folder format now YYYYMMDD_###
# v6.2.0.10 - fixed OB-663. Added = sign to both checks: if( ... => $minimumFolderFileLimit ) and ( ... <= $maximumFolderFileLimit ) )
# v6.2.0.11 - Changed default UseFolderFileLimit value to be =0
# v6.2.0.12 - Fixed Jira OB-672
# v6.2.0.13 - Fixed Jira OB-674 - FTP&CMB-903 not using UseFolderFileLimit option
# v6.2.0.14 - TP 19781 - New requirements for PID file naming format
# v6.2.0.15 - Fixed Jira OB-689 - DFL: When a file already exist in a output folder, it should be overwritten and the running file count should not be increased
# v6.2.0.16 - TP 20260 - New requirements for PID file naming format applied in tmp file
# v6.2.0.17 - Fixed Jira OB-709 - If used as output split mode the DFL does not get applied
# v6.2.0.18 - Fixed Jira OB-688 - New INI option PrependHostname to add (prepend) the hostname/ip-address to the filename
# v6.2.0.19 - Added extra functionality to compress files on ssh with the intention to help/fix Jira OB-733 - SFTP slow.
# v6.2.0.20 - Fixed Jira OB-818 - Application not decrypting password correctly when there are multiple usernames and passwords
# v6.2.0.21 - Fixed Jira OB-834 - Add new secure package usage Net::SFTP:Foreign
# v6.2.0.22 - Fixed Jira OB-867 - FTP fails with error "1 at (eval 1) line 279" if SFTPcompression is absent from ini file
# v6.2.0.23 - Fixed Jira OB-487 - FTP exe - File is NOT within size range
#                                                 OB-504 - FTP error - 'file size is too big > 200000000'
# v6.2.0.24 - Fixed Jira OB-892 - FTP: On using SFTP=1 and sftpcompression=3 (Wrong value), application crashing leaving behind a pid file
# v6.2.0.25 - Fixed Jira OB-949 - FTP - functionality to replace and remove text /expressions in the downloaded file names
# v6.2.0.26 - Fixed Jira OB-1028 - FTP - fails when new option RemoveFromFileName is not set
# v6.2.0.27 - Fixed Jira OB-1029 - Defer AI_heartBeat if it was run recently, according to new 'HeartBeatDeferTime' option
#                                - More efficient test for empty directory
# v6.2.0.28 - Fixed Jira OB-1099 - new feature to prepend the hostname when sending the file to the FTP backup
# v6.2.0.29 - Fixed Jira OB-1107 - Optima Backend applications need to exit gracefully if the PID file is removed [Scripts]
# v6.2.0.30 - Fixed Jira OB-1143 - FTP to tar files being copied to backup
# v6.2.0.31 - part fixed Jira OB-1148 - FTP to divert output to alternative file system when disk usage threshold exceeded
#
# v6.2.0.32 - Fixed Jira OB-1148 - added locks to archive (tar) files to prevent them being used before FTP has finished with them (backup and alternative directories)
#                                  [ file is xxx.tar.temp until FTP finished, then renamed to xxx.tar]
#                                - added SVN revision into the version string as eg v6.2.0.32.xxxx
#                                - FTP tars files output to the Alternative ("overload") filesystem
#                                - Alternative filesystem works with multiple output directories
# v6.2.0.33 - Fixed Jira OB-1155 - On Prependhostname=1 in the alternate output dir the IP gets added twice to the name of the tar file.
#           - Fixed Jira OB-1156 - In the alternate dir, the tarball contains only 1 file for same period...
#           - Fixed Jira OB-1157 - FTP: When files are ftpied in the alternate directory they remain in the download folder
# v6.2.0.34 - Fixed Jira OB-1156 - Better quoting for the tar command (to avoid problems with shell-significant characters like '|')
# v6.2.0.35 - Fixed Jira OB-1189 - FTP: archiveBackup=1, does not work [when UseFolderFileLimit is NOT set]
# v6.2.0.36 - Fixed Jira OB-1413 - FTP binary to have option to prepend collect date time to a file name
# v6.2.0.37 - Fixed Jira OB-1491 - RTB UAT : Health Checks for ERICSSON_UTRAN.RBS_CARRIER ( ftp slow)
#                                - Introduced new sftp option SFTP=3, which uses an external sftp program
#                                  specified by INI option SFTPcommand (default /usr/bin/sftp). Currently requires key-based authentication
#                                  (same as for SFTP=2).
# v6.2.0.38 - Fixed Jira OB-1559 - FTP - introduce multiple processes to improve performance (speed)
#                                - Introduced new options MaxProcesses : Maximum number of additional sub-processes (so 0 for original behaviour)
#                                  ProcessLevel : Directory level at which FTP will split into multiple processes
#                                  eg root/subdir[1..99]/subsubdir[1..99]/file1..file99 could choose
#                                     ProcessLevel=1 to split at subdir level, or 2 to split at subsubdir level
#                                  ("SFTP=1 option is not supported with MaxProcesses > 0)
# v6.2.0.39 - Fixed Jira OB-1613 - Optima FTP program should support non-default port number
#                                - New ability to supply a port number with the hostname, through the INI file
#                                  option remoteHost, in the form "Hostname:Port"
#                                - Improved error reporting on failed connections
# v6.2.0.40 - Fixed Jira OB-1750 - FTP downloads same file twice due to confused year in prepended timestamp
#                                  (make better guess for the year when it is not available in the remote timestamp)
# v6.2.0.41 - Fixed Jira OB-1883 - FTP V6.2 is producing End Program messages as WARNING instead of INFORMATION
#
# v7.0.0.0 - Target Process - #48911. FTP Put. Added upload functionality
# v7.0.0.1 - Fixed Jira OB-2507 - AUTOCLONE - FTP Log - filenames in 'downloaded' messages (V6.1.0.4)
# v7.0.0.2 - Fixed Jira OB-2462 - RemoveonDownload for files with pound (#) not working on Upload=1 and ReplacePoundWith=_
# v7.0.0.3 - Fixed Jira OB-2437 - FTP 'Put' should be fully supported on Windows
# v7.0.0.4 - Fixed Jira OB-2238 - The FTP application needs to support the put operation to allow files to be pushed to a separate location
# v7.0.0.5 - Fixed Jira OB-2440 - FTP: the PrependTImeStamp option does not seem to return be correct date time value
# v7.0.0.6 - Fixed Jira OB-955 - FTP: Secured FTP not working, application crashing leaving behind a prid file
# v7.0.0.7 - Fixed Jira OB-2440 - FTP: the PrependTImeStamp option does not seem to return be correct date time value
#									Added code --> if ($usingDefaultYear == 1 && $ai_FTPTimeFormat =~ /:/o && $ftptime =~ /^[0-9]{4}$/o )
# v7.0.0.8 - Fixed Jira OB-2462 - RemoveonDownload for files with pound (#) not working on Upload=1 and ReplacePoundWith=_
#									Added code --> if ($uploadDir =~ /\//o) { # UNIX
# v7.0.0.9 - Fixed Jira OB-2675 - AUTOCLONE - ERICSSON_UTRAN: OPTIMA FTP output duplicated result files for a same source raw file
# v7.0.0.10 - Fixed Jira OB-2673 - FTP: On using SFTP=3, PUT Functionality is not moving files from one server to another
#
# v7.0.1.0 - Fixed Jira OB-2717 - AUTOCLONE - opx_FTP_GEN_302 needs to be able to configure INI file to use the sftp switch -oBindAddress
# V7.0.1.1 - Fixed Jira OB-2753 - FTP does not unzip files before putting them into tar archive during overload operation.
#                                  New INI option alternativeOutputExtractBeforeArchive (default 0) to unzip/untar before archiving.
#                                  Fixed DFL bug (would go over DFL limit when extracting from tar files).
# V7.0.1.2 - Fixed Jira OB-2061 - ALU UTRAN NodeB Data Files Not downloaded for all NodeBs on the OSS with FTP version V6.2.0.40.3516
#                                  Moved clear of download queue to after test for "already running"
# V7.0.1.3 - Fixed Jira OB-3028 - AUTOCLONE - FTP bug in dateformat for Kodiak_PTT Interface.
#                                  New options can be used such as dateFormat=M-D-YYYY and dateFormat=M_D_YYYY
# V7.0.1.4 - Fixed Jira OB-3043 - AUTOCLONE - FTP CWD to.....command failed: causes FTP to crash
# V7.0.1.5 - Fixed Jira OB-2823 - FTP & DST issue when using Windows FTP server
# V7.0.1.6 - Fixed Jira OB-3183 - Data downloading is slow for SFTP parameter as 1 for ERICSSON_UTRAN interface 
#                                  (add new mode SFTP=4 as for SFTP=2 but with password instead of public/private ssh key)
# V7.0.1.7 - Fixed Jira OB-3179 - Duplicate files downloaded after OSS upgrade
#                                  (add new mode PrependTimestamp=2 to prepend timestamps to filenames but not in the list file,
#                                   add new option DownloadIfFileSizeChanged to add the filesize to the list file)
# V7.0.1.8 - Fixed Jira OB-3197 - FTP program is not processing date formats correctly - Fix order of tests for dateformat.
# V7.0.1.9 - Fixed Jira OB-3231 - FTP logs need to be tidied up to remove duplicate messages
#  7.1.0.0 - version number change for Optima 7.1 baseline
# 7.1.0.1 - Enhancement Jira OB-3382 (clone OB-3369) - multiple stacked NFS calls per write - We need a reduction of writes. Reduce occurrences of file open/close (maintain handles).
# 7.1.0.3 - Fixed Jira OB-3468 (Clone OB-3462) - BindAddress functionality does not work in FTP ini configuration
#
#
# 
#
# TODO
#      Check for availability of df command (diskUsageCommand) on current system (eg Windows...)
#      NB tar of output (for overload alt dir) will ignore auto untar/unzip options (unzipCommand, untarCommand, unzipAfterTar)
#
#
#
#   - The next available LOG message ID will be: 302128
#
#
#

my $mainPID = $$;

use IO::Handle;
use DirHandle;
use Net::FTP;
#use Net::SFTP::Foreign; # only for debug purpose when using IDE
#use Net::SFTP::Foreign::Compat; # only for debug purpose when using IDE
use File::Basename;
use File::Copy;
use File::Spec;
use Time::Local;

# for DFL - Directory File Limit
use Time::HiRes qw( gettimeofday ); # to get the date and time in milliseconds
use POSIX qw(ceil floor); # to round the date time

my $log_started = 0;

my $currentOutFolderFileCount=0; # holds the current Out folder file count
my $currentOutFolderName; # the current Out folder name

##my $currentBackupFolderFileCount=0; # holds the current Backup folder file count
##my $currentBackupFolderName; # the current Backup folder name

my $minimumFolderFileLimit = 100; # added 6.2.0.7
my $maximumFolderFileLimit = 100000; # added 6.2.0.7

# $minimumFolderFileLimit = 1; # testing
# $maximumFolderFileLimit = 5; # testing

my $debugAltFS = 0;

my $bFirstFile = 0;
my $bFirstFileBackup = 0;

my $ai_useDate = 0;

my $hostName = ""; # global var so that can be used by pid and log

my $ai_remoteHost;
my $ai_remoteHostReplaced="";

my $opxProgramName='FTP-GEN-010-GEN-302';

my $opxVersion = "7.1.0.3";

my $ai_opxFTP_version=$opxVersion;

my $ai_SVN_revision='$Rev: 7752 $';

{
    my $rev = '0000';
    $ai_SVN_revision =~ /\$Rev\: *(\d*)/;
    $rev=$1 if ($1);
    $ai_opxFTP_version .= ".$rev";
}


## 3.4-4 Handle signal interupts - set variable to stop loops.
my $ai_sigint=0;
# SIGHANDLE
sub sig_handler {
    $ai_sigint=1;
}
##--

my ($ai_prid, $ai_remoteDIR, $ai_fileMask, $ai_dirMask, $ai_numberofDays,$ai_SFTP);

my $AI_Log_Severity;

sub dbgprint {
#    my $x=shift; print "$$: $x";
#    AI_logMessage ("dbgprint --- $$: $x", 302999, $ai_prid, 5) if $log_started;
}

$hostName = substr(`hostname`, 0, -1); # substring to remove last char line feed
if ( $hostName eq "" ) {
    print "HOSTNAME environment variable not found!\n" if ($ai_verbose);
    die "HOSTNAME environment variable not found!\n";
}

# Load Configuration Options
if ($#ARGV < 0) {
    print "FTP: ".$opxProgramName."-".$ai_opxFTP_version."\n"."USAGE: <exe file name> -v\n"."       <exe file name> <ini file name>\n\n";
    exit;
}
if ($#ARGV >= 0 and $ARGV[0] eq "-v") {
    print "FTP: ".$opxProgramName."-".$ai_opxFTP_version."\n\n";
    exit;
}

my $debugtest = 0;
$debugtest = 1 if ($#ARGV >=1 && $ARGV[1] eq "-d");

undef %INI;
open IFH, "$ARGV[0]" or die "$ARGV[0]: Cannot open .ini file";
while (<IFH>) {
    if (/^\s*[#\[;]/o) {
        next;
    }

  # remove remaining hash-comments
    s/#.*//o;

    if (/\s*(\w+)\s*=\s*([\S ]*\S)\s*#/o) {
        $INI{$1}=$2;
    }
    elsif (/\s*(\w+)\s*=\s*([\S ]*\S)\s*$/o) {
        $INI{$1}=$2;
    }
    else {
        next;
    }
    dbgprint "--- INI $1 => $2\n";
}
close IFH;
foreach $key (sort keys %INI) {
## Win %ENV% , Unix $ENV
    if ($INI{$key} =~ /[%\$](\w+)%?/o) {
        if (exists $ENV{$1} and $ENV{$1} ne "") {
            $INI{$key} =~ s/[%\$](\w+)%?/$ENV{$1}/;
        }
    }
}
#--
#check and assign key parameters

sub setIniOrDefault {
    my $varRef = shift;
    my $key = shift;
    my $value = shift;

    if (exists $INI{$key}) {
        ${$varRef} = $INI{$key};
    } else {
        ${$varRef} = $value;
    }
}

sub setIniOrDie {
    my $varRef = shift;
    my $key = shift;
    my $message = shift;

    if (exists $INI{$key}) {
        ${$varRef} = $INI{$key};
    } else {
        die "$message";
    }
}

sub CheckForBoolValues
{
	my $value = shift;
	my $option = shift;
	if(($value<0) or ($value>1)) {
		die "\n$ARGV[0] \nERROR: The value allowed for  $option  is 0 (false) or 1 (true) ";
	}
}

my $defaultFTPDirMatch="d........."; # not "drwx"
my $ai_vmsStyle = 0;
my $ai_CheckingFileSize = 1;

my (@ai_remoteHosts, @ai_username, @ai_password, @ai_truePassword, $using_password);
my $ai_remoteArchDir="";
my $ai_encryptedPasswords;

# set of names of *.tar.temp, to be renamed to *.tar when ready for use by next program in the chain (when ftp finished)
my %archiveLocks;

my ($ai_archivePeriodMask, $ai_archiveMaxFilesInTar, $ai_archiveMaxSizeInTar, $archivePeriodRE);

my ($ai_archiveBackup, $ai_Tar);

my (%backupArchiveLocation, %backupArchiveFiles, %backupArchiveSize, %backupArchiveTarCount);
my @backupArchiveInfo = ( \%backupArchiveLocation, \%backupArchiveFiles, \%backupArchiveSize, \%backupArchiveTarCount );

my (%altArchiveLocation, %altArchiveTarCount, %altArchiveFiles, %altArchiveSize);
my @altArchiveInfo = ( \%altArchiveLocation, \%altArchiveFiles, \%altArchiveSize, \%altArchiveTarCount );

my ($ai_maxOutputFS, $ai_minOutputFS);

my (@ai_localDIR, @ai_altDIR);

my $missingPID=0;

my $extSFTP_executable;

# *** TODO configurable??
my $extSFTP_lsCommand = "ls -l";

my $ai_WindowsDSTFix;

my $ai_PrependTimestamp; 
my $ai_NoteFileSize;

## Processing parameters

my $max_subprocs;
my $max_connections;
my @ai_usedConnection;
my $fork_level;
my $parentOwnsConnections = 1;
my $subprocs = 0;

my $deferAllAtEnd = 1; # 0 (extra process for download) is not yet supported
my $ftpfile_pid = 0;

setIniOrDefault(\$max_subprocs, 'MaxProcesses', 0);
$max_connections = $max_subprocs + 1; # one for the main procedure
$max_connections += 1 if (!$deferAllAtEnd); # one for the ftpfile process

for (my $i=0; $i<$max_connections; ++$i) {
  $ai_usedConnection[$i] = 0;
}

if ($max_subprocs > 0) {
  setIniOrDie(\$fork_level, 'ProcessLevel', "MaxProcesses > 0 requires ProcessLevel set to the directory level at which FTP will split into multiple processes");
}

setIniOrDie(\$ai_SFTP, 'SFTP', "\n$ARGV[0]: No SFTP Flag");

if ($ai_SFTP < 0 || $ai_SFTP > 4) {
  die "SFTP option only supports values 0,1,2,3 and 4";
}

# Roll SFTP=4 into SFTP=2 having noted password option

if ($ai_SFTP == 0 || $ai_SFTP == 1 || $ai_SFTP == 4) {
  $using_password = 1;
} else {
  $using_password = 0; # private/public key method
}

if ($ai_SFTP == 4) {
  $ai_SFTP = 2;
}


if ($ai_SFTP==1 && $max_subprocs > 0) {
  die "SFTP=1 option is not supported with MaxProcesses > 0";
}

setIniOrDefault(\$extSFTP_executable, 'SFTPcommand', "/usr/bin/sftp");

setIniOrDie(\$ai_prid,'PRID',"\n$ARGV[0]: No PRID assigned");

setIniOrDefault(\$ai_archiveBackup, 'archiveBackup', 0);

# If diverting to the alternative directory, then we will archive (tar)
# (could make this optional if desired - would need to check conditions/DFL code below)
my $ai_archiveAlt = 1;

setIniOrDefault(\$ai_archivePeriodMask, 'archivePeriodMask', '.*');
# default has no (..), so will not match any, so all go in one tar (up to limits)
$archivePeriodRE = eval { qr/$ai_archivePeriodMask/ } ; # compile regular expression
if ($@) { # if an error message was set
  die "\nError: Bad regular expression (archivePeriodMask) '$ai_archivePeriodMask', exiting!\n";
}
#dbgprint "mask = qr/$ai_archivePeriodMask/ \n";

setIniOrDefault(\$ai_archiveMaxFilesInTar, 'archiveMaxFiles', 100);
setIniOrDefault(\$ai_archiveMaxSizeInTar,  'archiveMaxSize',  100000000);

setIniOrDefault(\$ai_WindowsDSTFix, 'WindowsDSTFix', 0);

#my $ai_timestamp;

#sub set_global_timestamp
sub set_datetimestamp
{
  my $datetimestamp_ref = shift;
  my($s,$mi,$h,$d,$m,$y)=(localtime)[0,1,2,3,4,5];
  $y+=1900; $m++;
  #$ai_timestamp = sprintf("%04d%02d%02d%02d%02d%02d",$y,$m,$d,$h,$mi,$s);
  ${$datetimestamp_ref} = sprintf("%04d%02d%02d%02d%02d%02d",$y,$m,$d,$h,$mi,$s);
}

setIniOrDefault(\$ai_Tar, 'archiveCommand', '/bin/tar');

# maximum percentage of disk usage for the filesystem of the normal output directory before diverting to alternative output directory
setIniOrDefault(\$ai_maxOutputFS, 'maxOutputFilesystemPercent', '90');

# maximum percentage of disk usage for the filesystem of the normal output directory before reverting to it from the alternative output directory
$ai_revertOutputFS = $ai_maxOutputFS - 1;

# maximum percentage of disk usage for the alternative file system before aborting
setIniOrDefault(\$ai_maxAltFS, 'maxAlternativeFilesystemPercent', '90');

# disk usage command that returns a line containing the percentage usage of the current directory (marked with % sign)
# eg (Solaris, Linux?)  /dev/md/dsk/d0       211757965 188091610  21548776  90% /data
#    (HP-UX)            58 % allocation used
setIniOrDefault(\$ai_df, 'diskUsageCommand', 'df -k . | tail -1');

setIniOrDefault(\$ai_localFile,'localFile',0);
if ($ai_localFile) {
    setIniOrDie(\$ai_localFileList,'localFileList', "\n$ARGV[0]: No localFileList assigned");
}

if ($ai_localFile && ($ai_SFTP >= 1)) {
    die "Only one of SFTP > 0 and localFile is allowed\n";
}

setIniOrDefault(\$ai_startOnDay,'startOnDay',0);

setIniOrDie(\$ai_numberOfDays,'numberOfDays', "\n$ARGV[0]: No numberOfDays assigned");
setIniOrDie(\$ai_datedDir,'datedDir', "\n$ARGV[0]: No datedDir assigned [0=False|n=Depth from remoteDir]");
setIniOrDie(\$ai_dateFormat,'dateFormat', "\n$ARGV[0]: No dateFormat assigned");
my $ai_noFilesInList;
if ($ai_dateFormat eq "0") {
    if (exists $INI{'noFilesInList'})
    {
        $ai_noFilesInList = $INI{'noFilesInList'};
    } else {
        die "\n$ARGV[0]: dateFormat=0 but noFilesInList was not assigned";
    }
} #3.2-3
###if ($ai_dateFormat eq "0") { setIniOrDefault(\$ai_noFilesInList,'noFilesInList',0 but noFilesInList was not assigned"); } #3.2-3

setIniOrDefault(\$ai_verbose,'verbose',0);
setIniOrDefault(\$ai_debug,'debug',0); ##3.4-5
setIniOrDefault(\$ai_Log_Severity,'LogSeverity',2); #3.17

if ($ai_Log_Severity < 1 or $ai_Log_Severity > 6 ) {
    die "\n$ARGV[0]: Log Severity can only have values in between 1 and 6.";
}

setIniOrDefault(\$ai_backup,'backup',1);
setIniOrDefault(\$ai_useMonitorFile,'useMonitorFile',1); ##3.0-9

if (exists $INI{'unzipCommand'}) {
    $ai_unzipCommand = $INI{'unzipCommand'};
    setIniOrDie(\$ai_zipExtension,'zipExtension', "\n$ARGV[0]: unzipCommand is set with no zipExtension");
    setIniOrDefault(\$ai_unzipAfterTar,'unzipAfterTar',0);
}
if (exists $INI{'untarCommand'}) {
    $ai_untarCommand = $INI{'untarCommand'};
    if (exists $INI{'tarExtension'}) {
        $ai_tarExtension = $INI{'tarExtension'}
    } else {
        $ai_tarExtension = ".*";
    }
} ##3.4-1

print "opx_FTP_GEN_302 version $ai_opxFTP_version\n" if ($ai_verbose);
dbgprint "***\n";
dbgprint "*** opx_FTP_GEN_302 version $ai_opxFTP_version\n";
dbgprint "***\n";

#--
## Directory parameters
setIniOrDie(\$ai_optima ,'optimaBase', "\n$ARGV[0]: No optimaBase assigned");
if (not -d $ai_optima) {
    die "\n$ai_optima  - OPTIMA BASE Directory does not exist ";
} # air00091759 - 28/03/2007

my $ai_local_root = "$ai_optima/ftp/${ai_prid}";
if (exists $INI{'LogDirectory'}) {
    $ai_defaultLogDirectory = $INI{'LogDirectory'};
} else {
    $ai_defaultLogDirectory = "$ai_optima/log";
}
if (not -d $ai_defaultLogDirectory) {
    die "\n$ai_defaultLogDirectory  - LOG Directory does not exist ";
} # air00091759 - 28/03/2007

if (exists $INI{'ProcDirectory'}) {
    $ai_defaultProcDirectory = $INI{'ProcDirectory'};
} else {
    $ai_defaultProcDirectory = "$ai_optima/pids";
}

if (not -d $ai_defaultProcDirectory) {
    die "\n$ai_defaultProcDirectory  - PID Directory does not exist ";
} # air00091759 - 28/03/2007

setIniOrDefault(\$ai_upload,'upload',0);
CheckForBoolValues($ai_upload, 'upload');

if (exists $INI{'FTPOutDirectory'}) {
	@ai_localDIR = split(/,/,$INI{'FTPOutDirectory'});
} else {
	@ai_localDIR = ("$ai_local_root/in");
} ##3.4-3

if(!$ai_upload) {
	for ($i=0; $i <= $#ai_localDIR; $i++) {
		if (not -d $ai_localDIR[$i]){
			dieNicely("$ai_localDIR[$i]  - OUTPUT Directory does not exist ");
		} # air00091759 - 28/03/2007
	}
}

# Failing over to an alternative filesystem in case of exceeding usage in normal Output folders
my $ai_altFS;

my $ai_altExtractBeforeArchive;

# currently in operation
my $ai_altFS_diverting = 0;

setIniOrDefault(\$ai_altFS, 'alternativeOutputFilesystem', 0);
setIniOrDefault(\$ai_altExtractBeforeArchive, 'alternativeOutputExtractBeforeArchive', 0);

if ($ai_altFS) {
	if (!exists $INI{'FTPAltDirectory'}) {
		dieNicely("alternativeOutputFilesystem specified, but FTPAltDirectory not present");
	}
  
	@ai_altDIR = split(/,/,$INI{'FTPAltDirectory'});

	if ($#ai_localDIR != $#ai_altDIR) {
		# warning
		AI_logMessage ( "FTPAltDirectory entries do not correspond to FTPOutDirectory entries", 302002, $ai_prid, 2);
	}

	for ($i=0; $i <= $#ai_altDIR; $i++) {
		if (not -d $ai_altDIR[$i]) {
			dieNicely("$ai_altDIR[$i]  - Alternative OUTPUT Directory does not exist ");
		}
	}
}

#setIniOrDie(\$ai_SSHargs, 'SFTPcompression', 1); # OB-733
setIniOrDefault(\$ai_SSHargs, 'SFTPcompression', 1); # OB-867
if ( $ai_SSHargs != 0 and $ai_SSHargs != 1 )  { ## added check in v6.2.0.24 - OB-892
	$ai_SSHargs = 1;
	my ($tempLogMsg) = "SFTPcompression has got an unexpected value therefore the default value (1) was used. 0 or 1 can be used.";
	print "$tempLogMsg \n" if ($ai_verbose);
	AI_logMessage ( $tempLogMsg,  302003, $ai_prid, 1);
}

my $ai_BindAddress = "";
#SFTPBindAddress to be used in conjunction with SFTP
setIniOrDefault(\$ai_BindAddress, 'SFTPBindAddress', ""); # OB-1787

setIniOrDefault(\$ai_MAXFilesInDownloadDir, 'MAXFilesInDownloadDir', 0); #

my $ai_noOutputDirs = $#ai_localDIR+1;

my $ai_noAltDirs = $#ai_altDIR+1;

if ($ai_MAXFilesInDownloadDir !=  0) {
    $atleastOneOutputDirFilesUnderLimit = 0;

    #print "...$#ai_localDIR\n"  if ($ai_verbose);

    for ($i=0; $i <= $#ai_localDIR; $i++) {
        #if (not -d $ai_localDIR[$i]) { die "\n$ai_localDIR[$i]  - OUTPUT Directory does not exist "; } # air00091759 - 28/03/2007
        $ai_localDIRCount[$i]=getFileCount($ai_localDIR[$i]);
        #print "$i  -  $ai_localDIR[$i]  -  $ai_localDIRCount[$i]\n"  if ($ai_verbose);
        AI_logMessage ("File Count is $ai_localDIRCount[$i] in $ai_localDIR[$i]",  302004, $ai_prid, 0);
        if ($ai_localDIRCount[$i] < $ai_MAXFilesInDownloadDir) {
            $atleastOneOutputDirFilesUnderLimit = 1;
        }
    }
    if($atleastOneOutputDirFilesUnderLimit == 0) {
        AI_logMessage ("All Download Directories have already no of files equal to $ai_MAXFilesInDownloadDir",  302005, $ai_prid, 5);
        die "\nAll Download Directories have already no of files equal to $ai_MAXFilesInDownloadDir";
    }
}
if (exists $INI{'FTPDownloadDir'}) {
    $ai_localTempDIR = $INI{'FTPDownloadDir'};
} else {
    $ai_localTempDIR = "$ai_local_root/tmp";
}

if (not -d $ai_localTempDIR) {
    die "\n$ai_localTempDIR  - DOWNLOAD Directory does not exist ";
} # air00091759 - 28/03/2007

if (exists $INI{'FTPErrorDir'}) {
    $ai_localErrorDIR = $INI{'FTPErrorDir'};
} else {
    $ai_localErrorDIR = "$ai_local_root/error";
}

if (not -d $ai_localErrorDIR) {
    die "\n$ai_localErrorDIR  - ERROR Directory does not exist ";
} # air00091759 - 28/03/2007

if (exists $INI{'FTPFileListDir'}) {
    $ai_listDirectory = $INI{'FTPFileListDir'};
} else {
    $ai_listDirectory = "$ai_local_root/list";
}

if (not -d $ai_listDirectory) {
    die "\n$ai_listDirectory  - LIST Directory does not exist ";
} # air00091759 - 28/03/2007

if (exists $INI{'FTPBackupDir'}) {
    $ai_backupDirectory = $INI{'FTPBackupDir'};
} else {
    $ai_backupDirectory = "$ai_local_root/backup";
}

if (not -d $ai_backupDirectory) {
    die "\n$ai_backupDirectory  - BACKUP Directory does not exist ";
} # air00091759 - 28/03/2007

if ($ai_noOutputDirs > 1 and -f "$ai_listDirectory/lastoutdir") {
    open TFH, "$ai_listDirectory/lastoutdir";
    $dir_offset=<TFH>;
    chomp $dir_offset;
    close TFH;
} else {
    $dir_offset=0;
}

#--
## Filename parameters
setIniOrDie(\$ai_fileMask, 'fileMask', "\n$ARGV[0]: No fileMask assigned");
setIniOrDefault(\$ai_excludeMask,'excludeMask',"^\$"); ##3.2-5
setIniOrDie(\$ai_dirMask, 'dirMask', "\n$ARGV[0]: No dirMask assigned");
if (exists $INI{'tosubdir'}) {
    $ai_tosubdir = $INI{'tosubdir'};
} else {
    $ai_tosubdir = 0;
} ##3.3-1
if (exists $INI{'fileDelim'}) {
    $ai_fileDelim = $INI{'fileDelim'};
} else {
    $ai_fileDelim = "_";
} ##3.3-1

setIniOrDefault(\$ai_MAXFileSize, 'MAXFileSize', 2000000000); # v6.2.0.22 - OB-487
setIniOrDefault(\$ai_MINFileSize, 'MINFileSize', 0); # v6.2.0.22 - OB-487

setIniOrDefault(\$ai_UseFolderFileLimit, 'UseFolderFileLimit', 0); # added v6.2.0.8
CheckForBoolValues($ai_UseFolderFileLimit, 'UseFolderFileLimit');

# hash with direstories in case the output split functionality is used and DFL is used too
my %dirs_hash;

if ($ai_UseFolderFileLimit > 0) { ## added check in v6.2.0.13
    if (exists $INI{'FolderFileLimit'}) { # currentFolderFileLimit = the max number of files in current folders
        # v6.2.0.10 - added = sign to both checks & for v6.2.0.12 changed from => to >=
        if ( ( $INI{'FolderFileLimit'} >= $minimumFolderFileLimit ) and ( $INI{'FolderFileLimit'} <= $maximumFolderFileLimit ) ){
            $ai_currentFolderFileLimit = $INI{'FolderFileLimit'};
        } else {
            die "\n$ARGV[0]: The value for the Folder File Limit must be in between ".$minimumFolderFileLimit." and ".$maximumFolderFileLimit;
        }
    } else {
        #$ai_currentFolderFileLimit = 0; # v6.2.0.6
        $ai_currentFolderFileLimit = 10000; # v6.2.0.7
    }

    if ($ai_noOutputDirs > 1) {
        for ($i=0; $i <= $#ai_localDIR; $i++) {
            # assign the key to the main hash and the inner hash will contain another hash to record the directories for DFL
            $dirs_hash{$ai_localDIR[$i]."/"}->{$ai_localDIR[$i]."/"} = 0;
        }
    } # if ($ai_noOutputDirs > 1)
}


if (exists $INI{'PrependSubDir'}) {
    @ai_PrependSubDir = split(/,/,$INI{'PrependSubDir'});
} else {
    undef @ai_PrependSubDir;
}
setIniOrDefault(\$ai_ReplaceColonWith,'ReplaceColonWith',"_"); ##3.0-1
setIniOrDefault(\$ai_ReplacePoundWith,'ReplacePoundWith',"_"); ## v6.2.0.25
setIniOrDefault(\$ai_RemoveFromFileName, 'RemoveFromFileName', undef); ## v6.2.0.25
if (exists $INI{'PrependSubStr'}) {
    $ai_PrependSubStr = $INI{'PrependSubStr'};
}
if (exists $INI{'PrependString'}) {
    $ai_PrependString = $INI{'PrependString'};
}
setIniOrDefault(\$ai_PrependSeparator,'PrependSeparator',"."); ##3.1-2

setIniOrDefault(\$ai_PrependTimestamp,'PrependTimestamp',0);##3.5-2
## new option PrependTimestamp=2, no longer "Bool" # CheckForBoolValues($ai_PrependTimestamp,'PrependTimestamp');

setIniOrDefault(\$ai_PrependHostname,'PrependHostname',0); ##OB-688
CheckForBoolValues($ai_PrependHostname, 'PrependHostname');

setIniOrDefault(\$ai_PrependCDT,'PrependCollectDateTime',0); ##OB-1413
CheckForBoolValues($ai_PrependCDT,'PrependCollectDateTime');

setIniOrDefault(\$ai_NoteFileSize, 'DownloadIfFileSizeChanged', 0);
CheckForBoolValues($ai_NoteFileSize, 'DownloadIfFileSizeChanged');

if (exists $INI{'AppendSubDir'}) {
    @ai_AppendSubDir = split(/,/,$INI{'AppendSubDir'});
} else {
    undef @ai_AppendSubDir;
} #3.17
if (exists $INI{'AppendString'}) {
    $ai_AppendString = $INI{'AppendString'};
} #3.17
if (exists $INI{'AppendBefore'}) {
    $ai_AppendBefore = $INI{'AppendBefore'};
} #3.17
setIniOrDefault(\$ai_AppendSeparator,'AppendSeparator',"."); #3.17
if (exists $INI{'AppendSubStr'}) {
    $ai_AppendSubStr = $INI{'AppendSubStr'};
} #3.17

if (exists $INI{'removeZipExtBeforeMatch'}) {
    $ai_removeZipExt = $INI{'removeZipExtBeforeMatch'};
    if ($ai_zipExtension eq "") {
        die "\n$ARGV[0]: removeZipExtBeforeMatch is set with no zipExtension";
    }
} else {
    $ai_removeZipExt=0;
} ##3.6-1

setIniOrDefault(\$ai_heartBeatDeferTime, 'HeartBeatDeferTime', 5);

#--
## FTP parameters
if (!$ai_localFile)
{
    # setIniOrDefault(\$ai_encryptedPasswords, 'encryptedPass', 0);
    $ai_encryptedPasswords = 1; # mandatory
	#$ai_encryptedPasswords = 0; # mandatory

    setIniOrDie(\$temp_rem_hosts,'remoteHost',"\n$ARGV[0]: No remoteHost assigned");
    @ai_remoteHosts = split(/,/,$temp_rem_hosts);

    setIniOrDie(\$temp_username,'remoteUser', "\n$ARGV[0]: No remoteUser assigned");
    @ai_username = split(/,/,$temp_username);

	if ($using_password) {
		setIniOrDie(\$temp_password,'remotePass', "\n$ARGV[0]: No remotePass assigned");
    	@ai_password = split(/,/,$temp_password);

		if ($ai_encryptedPasswords) {
        	if (!defined(&PLEX_Decode)) {
			  dieNicely("$ARGV[0]: Unable to read remotePass ");
			  # This probably means user isn't running the script in its wrapped form
			  # after using the PLEX tool (with DECRYPT option)
			}

			# it needs to loop as the crypter.dll is able to crypt a list of comma separated psw but it is not returning comma separated encrypted psw
			for (my $i=0; $i < scalar (@ai_password); ++$i) {
			  $temp_password = PLEX_Decode($ai_password[$i]);
			  # save back to the array to be able to use it with its users
			  $ai_password[$i]=$temp_password;
			}
		  }

		#if ($#ai_remoteHosts != $#ai_username or $#ai_remoteHosts != $#ai_password) { #   the $#  is returning amount-1 with scalar is returning the correct value
		if (scalar(@ai_remoteHosts) != scalar(@ai_username) or scalar(@ai_remoteHosts) != scalar(@ai_password)) {
		  die "\n$ARGV[0]: Username or password lists do not match the remoteHost list";
		} ##SBG v3.16
	}
}

setIniOrDie(\$temp_remotedir,'remoteDir', "\n$ARGV[0]: No remoteDir assigned");
@ai_remoteDIR = split(/,/, $temp_remotedir);

if($ai_upload) {
    setIniOrDie(\$temp_remotedir,'FTPInDirectory', "\n$ARGV[0]: No Input Dir assigned");
	dbgprint "temp_remotedir/FTPInDirectory:   $temp_remotedir\n";
	
    @ai_remoteDIR = split(/,/, $temp_remotedir);
    
    if (exists $INI{'remoteDir'}) {
        @ai_localDIR = split(/,/,$INI{'remoteDir'});
    } else {
        @ai_localDIR = ("$ai_local_root/in");
    }

	dbgprint "ai_localDIR: @ai_localDIR\n";
	setIniOrDie(\$ai_uploadFileList,'uploadFileList', "\n$ARGV[0]: No uploadFileList assigned");
	
	$ai_altFS = 0;
	
	$extSFTP_lsCommand = $ai_uploadFileList;
}

if (exists $INI{'remoteArchiveDir'}) {
    $ai_remoteArchDir = $INI{'remoteArchiveDir'};
}  ##3.6-2
setIniOrDefault(\$ai_removeOnDownload,'removeOnDownload',0); ##3.3-2
setIniOrDefault(\$ai_FTPTimeOffset,'FTPTimeOffset',0); ##3.0-4
setIniOrDefault(\$ai_FTPSafetyPeriod,'FTPSafetyPeriod',0); ##3.0-4
setIniOrDie(\$ai_FTPStyle,'FTPStyle', "\n$ARGV[0]: No FTPStyle assigned");
setIniOrDefault(\$ai_FTPType,'FTPType',"ASCII"); ##3.0-8
setIniOrDefault(\$ai_FTPActive, 'FTPActive', 0); # Default 0, use passive ftp. If 1, use active ftp.

if ($ai_SFTP == 3 && $ai_FTPStyle eq "stdWINDOWS") {
  die "\nSFTP option 3 is not supported on Windows systems\n";
}

## 3.2-4  - TO complete

sub setFTPParamsFromStdStyle {
    my $dateTimeStyle = shift;
    my $dateFormat = shift;
    my $timeFormat = shift;
    my $nonDateTimeStyle = shift;
    my $dirMatch = shift;

    if ($ai_FTPSafetyPeriod >= 1 or $ai_PrependTimestamp >= 1) {
        # using DATE, TIME
        $ai_FTPStyle = $dateTimeStyle;
        setIniOrDefault( \$ai_FTPDateFormat, 'FTPDateFormat', $dateFormat );
        setIniOrDefault( \$ai_FTPTimeFormat, 'FTPTimeFormat', $timeFormat );
    } else {
        # not using DATE, TIME
        $ai_FTPStyle=$nonDateTimeStyle;
    }

    setIniOrDefault( \$ai_FTPDirMatch, 'FTPDirMatch', $dirMatch );
}

if ($ai_FTPStyle eq "stdUNIX") {
	if( $temp_remotedir =~ /\//o ) { # check remote dir for UNIX format - OB-504
			setFTPParamsFromStdStyle( "DIR,X,X,X,SIZE,DATE,TIME,NAME",
										"Mth3 D", "HH24:MI",
										"DIR,X,X,X,SIZE,X,X,X,NAME",
										"$defaultFTPDirMatch" );
	} elsif ($temp_remotedir =~ /\\/o) { # check remote dir for WIN format - OB-504
		dieNicely("The FTP Style \"Unix\" if different than the entered connection details as \"Windows\". Please check INI file for consistence and try again!");
	}
} elsif ($ai_FTPStyle eq "stdWINDOWS") {
    # (3.26): don't think DOS style DIR listings have been fully tested
    # - fails to interpret correctly file sizes with commas, eg
    #  02/06/2008  12:47            63,046 opx_FTP_GEN_302.pl
    # fails to read number 63046
    # - tries to interpret non-file lines such as
    #  Volume Serial Number is...
    #  nn Dir(s)  xxx,xxx,xxx bytes free
    # etc
    # also directories, subdirs, concatenation abc\def\ghi/poi problem
    #AI_logMessage("*** Warning: FTPStyle stdWINDOWS is not fully supported",  3020xx, $ai_prid, 2);

    if ($temp_remotedir =~ /\\/o) { # check remote dir for WIN format - OB-504
                setFTPParamsFromStdStyle( "DATE,TIME,SIZEORDIR,NAME",
											"MM-DD-YY", "HH:MIAM",
#											"DD/MM/YYYY", "HH24:MI",
											#"DD/MM/YYYY", "HH:MIAM",
											"X,X,SIZEORDIR,NAME", ## original
											#"DATE,TIME,SIZEORDIR,NAME",
											"<DIR>" );
        } elsif( $temp_remotedir =~ /\//o ) { # check remote dir for UNIX format - OB-504
            dieNicely("The FTP Style \"Windows\" if different than the entered connection details as \"Unix\". Please check INI file for consistence and try again!");
            #AI_logMessage("The FTP Style \"Windows\" if different than the entered connection details as \"Unix\". Trying in any case.",  3020xx, $ai_prid, 2);
            #setFTPParamsFromStdStyle( "DATE,TIME,SIZEORDIR,NAME",
						#					"DD/MM/YYYY", "HH:MIAM",
						#					"X,X,SIZEORDIR,NAME",
						#					"<DIR>" );
        }
    $ai_WindowsDSTFix=1;
    AI_logMessage ("Set FTP Windows Style",  302006, $ai_prid, 0);
} elsif ($ai_FTPStyle eq "stdVMS") {
    $ai_vmsStyle = 1;
    setFTPParamsFromStdStyle( "NAME,X,DATE,TIME,X", # don't worry about extra ,X on end
                            "D-MTH3-YYYY", "HH24:MI:SS",
                            "NAME,X,X,X,X",
                            "$defaultFTPDirMatch" );
} else {
    if ($ai_PrependTimestamp >= 1) {
        if (not($ai_FTPStyle =~ /DATE/o and $ai_FTPStyle =~ /TIME/o)) {
            die "\n$ARGV[0]: FTPStyle requires DATE and TIME when PrependTimestamp specified";
        }
    } ## SG - 3.5-6

    if ($ai_FTPStyle =~ /DATE/o) {
        setIniOrDie(\$ai_FTPDateFormat, 'FTPDateFormat', "\n$ARGV[0]: FTPStyle specifies DATE and FTPDateFormat is not assigned");
    } ##3.0-4
    if ($ai_FTPStyle =~ /TIME/o) {
        setIniOrDie(\$ai_FTPTimeFormat, 'FTPTimeFormat', "\n$ARGV[0]: FTPStyle specifies TIME and FTPTimeFormat is not assigned");
    } ##3.0-4
    setIniOrDefault(\$ai_FTPDirMatch,'FTPDirMatch',$defaultFTPDirMatch); ##3.0-2
}

if (not ($ai_FTPStyle =~ /NAME/o)) {
        die "\n$ARGV[0]: FTPStyle requires NAME";
} ## SG - 3.0-2 Check for variables in FTPStyle
if (not ($ai_FTPStyle =~ /SIZE/o)) {
	$ai_CheckingFileSize = 0;
	AI_logMessage ("No SIZE in FTPStyle - will not check for file size",  302007, $ai_prid, 0);
}

#--


if ($ai_WindowsDSTFix == 1 && $ai_PrependTimestamp != 1) {
	AI_logMessage("Warning - WindowsDSTFix options not applicable because PrependTimestamp is not 1",  302008, $ai_prid, 2);
	$ai_WindowsDSTFix = 0;
}

if ($ai_NoteFileSize && !$ai_CheckingFileSize) {
  AI_logMessage("Warning - DownloadIfFileSizeChanged will not be effective because no SIZE in FTPStyle",  302009, $ai_prid, 2);
}


# End of Configuration options
# Global Variables

my %backupDFL;

$backupDFL{'ROOT'} = $ai_backupDirectory;

my @outputDFLs; # array of hashes according to output directories
my @altDFLs;    # array of hashes according to output directories on alternative filesystem

for (my $i=0; $i < $ai_noOutputDirs; $i++) {
	dbgprint "setting outputDFL root $i to $ai_localDIR[$i]\n";
	$outputDFLs[$i]{'ROOT'} = $ai_localDIR[$i];
}

for (my $i=0; $i < $ai_noAltDirs; $i++) {
	dbgprint "setting altDFL root $i to $ai_altDIR[$i]\n";
	$altDFLs[$i]{'ROOT'} = $ai_altDIR[$i];
}

my $scFileName;
my %fileTimes;
my %fileSizes;
my $ai_currentRemoteDir;
$ai_datetimeNow = time() - ($ai_FTPTimeOffset*3600);

#@downloadedFiles = ();
$ai_starttime = time;

#testing
if ($debugtest)
{
    $ai_realstarttime = $ai_starttime;
    $ai_starttime = 1391791732; # Friday,  7 February 2014 16:49:09 GMT
    AI_logMessage("Debug Test - setting start time to $ai_starttime",  999, $ai_prid, 2);
}

$subdir_level=0;
$totalkbytes=0;
if ($ai_PrependString) {
    $subdir_prepend=$ai_PrependString.$ai_PrependSeparator;
} else {
    $subdir_prepend="";
}
if ($ai_AppendString) {
    $subdir_append=$ai_AppendSeparator.$ai_AppendString;
} else {
    $subdir_append="";
}

@MONTHS=('January','February','March','April','May','June','July','August','September','October','November','December');
#@MTH=('Ja','Fe','Mr','Ap','Ma','Jn','Jl','Au','Se','Oc','No','De'); # commented out v3.17
#@MTH2=('Jn','Fb','Mr','Ap','MY','Jn','Jl','Au','Se','Oc','No','De'); # commented out v3.17

@MTH2=('Ja','Fe','Mr','Ap','My','Jn','Jl','Au','Se','Oc','No','De'); # v3.17 air00086973

# End of Global Variables

## 3.0-2 FTPStyle processing
$count=0;
$ai_dateCheck=0;
undef %FTPStylePos;
@FTPStyle_List = split(/,/, $ai_FTPStyle);
if ($ai_FTPStyle =~ /DATE/o) {
    @DateList = split(/\s+/, $ai_FTPDateFormat);
}
foreach $style (@FTPStyle_List) {
    if ($style =~ /NAME/o) {
        $FTPStylePos{'Name'}=$count;
    } elsif ($style =~ /SIZEORDIR/o) {
        $FTPStylePos{'Size'}=$count; $FTPStylePos{'Dir'}=$count;
    } elsif ($style =~ /SIZE/o) {
        $FTPStylePos{'Size'}=$count;
    } elsif ($style =~ /DIR/o) {
        $FTPStylePos{'Dir'}=$count;
    } elsif ($style =~ /DATEORTIME/o) {
        $FTPStylePos{'Date'}=$count; $FTPStylePos{'Time'}=$count; $ai_dateCheck=1; $count+=$#DateList
    } elsif ($style =~ /DATE/o) {
        $FTPStylePos{'Date'}=$count; $ai_dateCheck=1; $count+=$#DateList
    } elsif ($style =~ /TIME/o) {
        $FTPStylePos{'Time'}=$count; $ai_dateCheck=1;
    }
    $count++;
}
if ((exists $FTPStylePos{'Date'} or exists $FTPStylePos{'Time'}) and
        not (exists $FTPStylePos{'Date'} and exists $FTPStylePos{'Time'})) {
    die "\n$ARGV[0]: FTPStyle specifies TIME or DATE but not both";
}
$ai_StyleNoFields = $count;
#--
$count=0;
##3.1-1

my $file_count=0;
my $outputDirIndex=0;

##dbgprint " $ai_FTPDirMatch,$ai_removeOnDownload,$ai_AppendString,$ai_MINFileSize,$ai_verbose,$ai_SFTP  \n";
##exit;

########
######## Start Processing
########

my $ai_timestamp;
#set_global_timestamp();
set_datetimestamp(\$ai_timestamp);

AI_startProgram($ai_prid, 0, $ai_defaultProcDirectory) if ($ai_useMonitorFile);
AI_logMessage ("Started FTP Script (".join(',',@ai_remoteHosts)."): $ai_opxFTP_version", 302001, $ai_prid, 1);

if($ai_upload && $ai_UseFolderFileLimit>0){
	AI_logMessage ("The Folder File Limit (DFL) is not supported with the upload functionality", 302010, $ai_prid, 1);
	$ai_UseFolderFileLimit = 0;
}

my $ftpDownloadQueue = File::Spec->catfile($ai_listDirectory, "ftpDownloadQueue");
unlink "$ftpDownloadQueue";

# First get the files from the last listing
# Connect to host

sub AI_getConnection
{
	my $i;
	AI_logMessage("AI_getConnection", 302011, $ai_prid, 0);
	for ($i=0; $i < $max_connections; ++$i)
	{
		if (!$ai_usedConnection[$i])
		{
			# found a slot - do we need to initialize?
			if (!defined $ai_connection[$i])
			{
				$ai_connection[$i] = AI_connectToHost();
			}
			$ai_usedConnection[$i] = 1;
			$ai_currentConnection = $i; # only used for debugging at the moment
			$ai_ftpHandle = $ai_connection[$i];
			# all done
			AI_logMessage("AI_getConnection: return handle $ai_connection[$i] == $ai_ftpHandle, number $i", 302012, $ai_prid, 0);
			return $i;
		}
	}
	# didn't find one
	# don't expect to see this the way AI_getconnection is called at the moment
	AI_logMessage("AI_getConnection - none available", 302013, $ai_prid, 4);
	# * todo - could wait for connection to become free here
	return -1;
}

sub AI_releaseConnection
{
	$connectionNumber = shift;
	AI_logMessage("AI_releaseConnection $connectionNumber", 302014, $ai_prid, 0);
	$ai_usedConnection[$connectionNumber] = 0;
}

AI_getConnection();

$dateList="";

scReadIn() if ($ai_FTPSafetyPeriod);

my $ai_noFiles=0;
my $ai_noFilesPlusOneH=0;
my $ai_noFilesMinusOneH=0;
undef %lastFileList;
undef %lastFileListPlusOneH;
undef %lastFileListMinusOneH;
my $finishedLoop = 0;
for ($i=$ai_startOnDay; ($i < $ai_startOnDay+$ai_numberOfDays) && !$finishedLoop; $i++) {
    my $directoryName;
    my $dateDir = dir_date_name($i);
    my $local_useDate = file_date_name($i);
    $dateList .= $local_useDate." ";
    # Get lastFiles for $dateDir
    my ($ai_lastListFile, $ai_lastListFilePlusOneHour, $ai_lastListFileMinusOneHour);
    if ($ai_dateFormat eq "0") {
        $ai_lastListFile = "lastfiles.log";

        if($ai_WindowsDSTFix == 1) {
			$ai_lastListFilePlusOneHour = "lastfilesPlusOneH.log";
			$ai_lastListFileMinusOneHour = "lastfilesMinusOneH.log";
        }
        
        ##  $i=$ai_numberOfDays; ## Set to kill the loop
        $finishedLoop = 1;
    } else {
        $ai_lastListFile = "lastfiles.$dateDir";
        $lastFileList{$local_useDate}=$dateDir;
        AI_logMessage ("List: $local_useDate => $dateDir", 302015, $ai_prid, 0); # changed to DEBUG v3.17

        if($ai_WindowsDSTFix == 1) {
			$ai_lastListFilePlusOneHour = "lastfilesPlusOneH.$dateDir";
			$ai_lastListFileMinusOneHour = "lastfilesMinusOneH.$dateDir";
			
			$lastFileListPlusOneH{$local_useDate}=$dateDir;
			$lastFileListMinusOneH{$local_useDate}=$dateDir;
		}
	}
    my $fullLastListFile = "$ai_listDirectory"."/$ai_lastListFile";
    my ($fullLastListFilePlusOneH, $fullLastListFileMinusOneH);
    if($ai_WindowsDSTFix == 1) {
		$fullLastListFilePlusOneH = "$ai_listDirectory"."/$ai_lastListFilePlusOneHour";
		$fullLastListFileMinusOneH = "$ai_listDirectory"."/$ai_lastListFileMinusOneHour";
    }

	# not currently used # $ai_haveFTPedFiles = 0;    #Flag that is set once some files have been downloaded
    print "Processing Date: $dateDir\n" if ($ai_verbose);
    if (-f $fullLastListFile) {
        if(!(open (INPUT, "$fullLastListFile"))) {
            AI_logMessage ("Couldn\'t open file $fullLastListFile", 302016, $ai_prid, 5);
            AI_endProgram($ai_prid, $ai_defaultProcDirectory) if ($ai_useMonitorFile);
            dieNicely("Couldn\'t open file $fullLastListFile\n");
        }
        while (<INPUT>) {
            chomp($_);
            $ai_noFiles++;
            ## 3.2-1
            if ($ai_dateFormat eq "0") {
                $lastFileList{$_}=1; # load list from file
            } else {
                $lastFileList{$local_useDate}{$_}=1;
            }
        #--
        }
		close INPUT;
    }
	
	if($ai_WindowsDSTFix == 1) {
		if (-f $fullLastListFilePlusOneH) {
			if(!(open (INPUTPOH, "$fullLastListFilePlusOneH"))) {
				AI_logMessage ("Couldn\'t open file $fullLastListFilePlusOneH", 302017, $ai_prid, 5);
				AI_endProgram($ai_prid, $ai_defaultProcDirectory) if ($ai_useMonitorFile);
				dieNicely("Couldn\'t open file $fullLastListFilePlusOneH\n");
			}
			while (<INPUTPOH>) {
				chomp($_);
				$ai_noFilesPlusOneH++;
				if ($ai_dateFormat eq "0") {
					$lastFileListPlusOneH{$_}=1; # load list from file
				} else {
					$lastFileListPlusOneH{$local_useDate}{$_}=1;
				}
			}
			close INPUTPOH;
		}
	
		if (-f $fullLastListFileMinusOneH) {
			if(!(open (INPUTMOH, "$fullLastListFileMinusOneH"))) {
				AI_logMessage ("Couldn\'t open file $fullLastListFileMinusOneH", 302018, $ai_prid, 5);
				AI_endProgram($ai_prid, $ai_defaultProcDirectory) if ($ai_useMonitorFile);
				dieNicely("Couldn\'t open file $fullLastListFileMinusOneH\n");
			}
			while (<INPUTMOH>) {
				chomp($_);
				$ai_noFilesMinusOneH++;
				if ($ai_dateFormat eq "0") {
					$lastFileListMinusOneH{$_}=1; # load list from file
				} else {
					$lastFileListMinusOneH{$local_useDate}{$_}=1;
				}
			}
			close INPUTMOH;
		}
	}
}

#foreach $key (sort keys %lastFileList) {
#print "$key\n";
#  foreach $filer (sort keys %{ $lastFileList{$key} }) {
#print "$filer,$lastFileList{$key}{$filer}\n";
#  }
#}
## 3.2-2


# This is for use with the additional download (ftpfile_pid) process - not available yet
# sub processDownloadQueue
# {
# 
# # *** todo have to deal with all #file counts, times, write out to safetycheck, write to list file here
# # *** beware                    if ($f_count >= $ai_noFiles-$ai_noFilesInList) {
# # unless we pass these numbers up somehow
# # eg through the queue file at end
# 
# 	my ($filename, $filesize, $fullfilename, $currentDir, $usedate);
# 	my $alldone = 0;
# 
# 	open FH_DEFERRED, "$ftpDownloadQueue" or return -2;
# 
# 	while (!$alldone)
# 	{
# 		while (<FH_DEFERRED>)
# 		{
# 			chomp;
# 			if ( m,([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*), )
# 			{
# 				# matches (non-tab) tab (non-tab) tab (non-tab) tab (non-tab) tab (non-tab)
# 				# as per  print OUTFTPDEFER "$filename\t$filesize\t$fullfilename\t$currentDir\n";
# 
# 				($filename, $filesize, $fullfilename, $currentDir, $usedate) = ($1,$2,$3,$4,$5);
# 
# 				dbgprint "process Download Queue - deferred: $filename [$filesize]: $fullfilename - $currentDir [$usedate]\n";
# 				ftp_file($filename, $filesize, $fullfilename, $currentDir, $usedate);
# 			} else
# 			{
# 				AI_logMessage("processDownloadQueue - received signal to quit, ending", 3020xx, $ai_prid, 0);
# 				$alldone = 1;
# 			}
# 		}
# 		sleep 1 if !$alldone;
# 	}
# 	return 0;
# }

# This is for use with the additional download (ftpfile_pid) process - not available yet
# if (!$deferAllAtEnd)
# {
# 	# new process to actually get the files as we see them
# 
# 	# first clear the queue
# 	open OUTFTPDEFER, ">$ftpDownloadQueue" or dieNicely("Could not open ftpdeferlist file for truncate");
# 	close OUTFTPDEFER;
# 
# 	$oldmainhandle = $ai_ftpHandle;
# 	$connectionNumber = AI_getConnection();
# 
# 	$ftpfile_pid = fork();
# 	if ($ftpfile_pid == 0)
# 	{
# 		# child
# 		$res = processDownloadQueue();
# 		if (!$res)
# 		{
# 			AI_logMessage("processDownloadQueue returned with error $res", 3020xx, $ai_prid, 2);
# 		}
# 
# 		# *** todo write out sc, listfile(?), ...
# 
# 		AI_logMessage("processDownloadQueue finished, exiting process $ftpfile_pid", 3020xx, $ai_prid, 0);
# 		exit;
# 	} else
# 	{
# 		# parent
# 		AI_logMessage("parent $$ of ftpfile process $ftpfile_pid [$connectionNumber]", 3020xx, $ai_prid, 0);
# 		$ai_ftpHandle = $oldmainhandle;
# 	}
# }

#Now process the remote directories to download
foreach $ai_remoteDIR (@ai_remoteDIR) {
    if ($ai_sigint) {
        last;
    } ## 3.4-4
    if(!$ai_upload) {
		if (ftp_cd($ai_remoteDIR)) {
			if ($max_subprocs > 0) {
				processSubDir($subdir_level, $subdir_prepend, $subdir_append, $ai_remoteDIR, 0); # can't start with dated dir, let's say
			} else {
				processdir($subdir_level, $subdir_prepend, $subdir_append, $ai_remoteDIR, 0); # can't start with dated dir, let's say
			}
		}
    } else { # if upload
      	if ($max_subprocs > 0) {
			processSubDir($subdir_level, $subdir_prepend, $subdir_append, $ai_remoteDIR, 0); # can't start with dated dir, let's say
		} else {
    		processdir($subdir_level, $subdir_prepend, $subdir_append, $ai_remoteDIR, 0); # can't start with dated dir, let's say
        }
    }
}

if ($max_subprocs > 0)
{
	AI_logMessage("Waiting for $subprocs children to finish", 302019, $ai_prid, 0);
	while ($subprocs > 0)
	{
		$res = wait;
		if ($res == -1)
		{
			AI_logMessage("Unexpectedly ran out of children while waiting for $subprocs to finish", 302020, $ai_prid, 2);
			last;
		}
		if (!$deferAllAtEnd && $res == $ftpfile_pid)
		{
			AI_logMessage("Child $res (ftpfile) finished early", 302021, $ai_prid, 2);
			$ftpfile_pid = 0;
		} elsif (defined $connectionMap{$res})
		{
			AI_logMessage("Child $res finished", 302022, $ai_prid, 0) if ($res != -1);
			$subprocs--;
		} else
		{
			AI_logMessage("Child $res finished (not ours)", 302023, $ai_prid, 0) if ($res != -1);
		}
	} #  while ($res != -1);

	AI_logMessage("Finished waiting for children", 302024, $ai_prid, 0);
}

# close parent output handle on dlq if it exists
closeHandles("dlq");

#print "\n***** completed scan **************\n";
#AI_endProgram($ai_prid, $ai_defaultProcDirectory) if ($ai_useMonitorFile);
#exit 234;
#<STDIN>;


if ($deferAllAtEnd && ! $ai_sigint)
{
	dbgprint "deferAllAtEnd processing queue\n";
	# process ftpfilelist in one go
	my $res = open FH_DEFERRED, "$ftpDownloadQueue";
	if (defined $res && $res)
	{
	    my ($filename, $filesize, $fullfilename, $currentDir, $usedate, $fullfilenamePlusOneH, $fullfilenameMinusOneH, $fileNameForListFile);
		while (<FH_DEFERRED>)
		{
			chomp;
			if ( m,([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*), )
			{
			# matches (non-tab) tab (non-tab) tab (non-tab) tab (non-tab)
			# as per  print OUTFTPDEFER "$filename\t$filesize\t$fullfilename\t$currentDir\n";

				($filename, $filesize, $fullfilename, $currentDir, $usedate, 
				 $fullfilenamePlusOneH, $fullfilenameMinusOneH, $fileNameForListFile) = ($1,$2,$3,$4,$5,$6,$7,$8);
      			#dbgprint "deferred: $filename [$filesize]: $fullfilename - $currentDir   ------   ai_localDIR[0]: $ai_localDIR[0]\n";
				
				if(!$ai_upload) {
          #ftp_file($filename, $filesize, $fullfilename, $currentDir, $usedate, $dateWin, $timeWin);
					ftp_file($filename, $filesize, $fullfilename, $currentDir, $usedate, "", 0, $fullfilenamePlusOneH, $fullfilenameMinusOneH, $fileNameForListFile);
				} else {
					#dbgprint "upload - ai_localDIR[0]: $ai_localDIR[0]\n";
					ftp_file($filename, $filesize, $fullfilename, $currentDir, $usedate, $ai_localDIR[0], 0, $fullfilenamePlusOneH, $fullfilenameMinusOneH, $fileNameForListFile);
				}
			} else {
				dbgprint "failed to match '$_'\n";
				last;
			}
		}
	} else
	{
		AI_logMessage("No files in download queue", 302025, $ai_prid, 0);
		# (not a problem)
	}
} else
{
    # *** NOT CURRENTLY USED ***
	# !$deferAllAtEnd (extra download process)
	# signal to ftpfile process that we are done, time to quit
	open OUTFTPDEFER, ">>$ftpDownloadQueue" or dieNicely("Could not open ftpdeferlist file for appending");
	print OUTFTPDEFER "\n";
	close OUTFTPDEFER;
	# wait for it to finish
	while ($ftpfile_pid)
	{
		$res = wait;
		if ($res == -1)
		{
			AI_logMessage("Unexpectedly ran out of children while waiting for ftpfile ($ftpfile_pid) to finish", 302026, $ai_prid, 2);
			last;
		}
		if ($res == $ftpfile_pid)
		{
			AI_logMessage("Child $res (ftpfile) finished", 302027, $ai_prid, 0);
			last;
		}
	}
	# *** NOT CURRENTLY USED ***
}

unlink "$ftpDownloadQueue";

closeHandles("list");

#--

## TODO
## Move files in $ai_localTempDIR to $ai_localErrorDIR.

AI_ftpExit();

scWriteOut() if ($ai_FTPSafetyPeriod);

## 3.2-3
if ($ai_dateFormat eq "0") {
    $f_count=0;
    if ( -f "$fullLastListFile" ) {
        if(!(open (INPUT, "$fullLastListFile"))) {
            AI_logMessage ("Couldn\'t open file $fullLastListFile", 302028, $ai_prid, 5);
        } else {
            ##if(!(open (OUTPUT, ">$fullLastListFile.tmp"))) {

            ## fullLastListFile contains the full path and the lastfiles.log
            ##my ($tempFileName) = "${hostName}_"."${fullLastListFile}.tmp";
            my ($tempFileName) = "$fullLastListFile.tmp";

            if(!(open (OUTPUT, ">${tempFileName}"))) {
                close INPUT;
                AI_logMessage ("Couldn\'t open file $tempFileName", 302112, $ai_prid, 5);
            } else {
                my $howMany=0;
                while (<INPUT>) {
                    if ($f_count >= $ai_noFiles-$ai_noFilesInList) {
                        print OUTPUT $_;
                        $howMany++;
                    }
                    $f_count++;
                }
                close INPUT;
                close OUTPUT;

                move "$tempFileName", "$fullLastListFile";

                if ($howMany == 0) {
                    AI_logMessage("List file $fullLastListFile is now empty, removing", 302029, $ai_prid, 0);
                    unlink "$fullLastListFile";
                }
            }
        }
    } else {
        AI_logMessage("No List file $fullLastListFile to update", 302030, $ai_prid, 0);
    }
}
#--

if ($ai_noOutputDirs > 1) {
    open TFH, ">$ai_listDirectory/lastoutdir";
    print TFH (($file_count+$dir_offset)%$ai_noOutputDirs)."\n";
    close TFH;
}
AI_endProgram($ai_prid, $ai_defaultProcDirectory) if ($ai_useMonitorFile);
$ai_finishtime=time;

if ($debugtest) { $ai_timerun= $ai_finishtime-$ai_realstarttime; }
else {
$ai_timerun= $ai_finishtime-$ai_starttime;
}
if (!$ai_CheckingFileSize) {
    $totalkbytes="<unknown size>";
} else {
    # uncomment this if we want to round kbytes to integer
    #    $totalkbytes=int($totalkbytes);
    # this to format as 123.4 KB
    $totalkbytes=sprintf("%.1f",$totalkbytes);
}
AI_logMessage ("FTP Process completed (".join(',',@ai_remoteHosts).")", 302127, $ai_prid,1);
if(!$ai_upload) {
	AI_logMessage ("Time (sec): $ai_timerun - Files: $file_count - Downloaded (kB): $totalkbytes",  302031, $ai_prid, 1);
} else {
	AI_logMessage ("Time (sec): $ai_timerun - Files: $file_count - Uploaded (kB): $totalkbytes",  302032, $ai_prid, 1);
}

closeHandles("log");

exit;

######## End of Processing

sub PercentUsage {
	my $dir = shift;
	my $df = `cd $dir; $ai_df`;

	$df =~ /(\d*) ?%/o;   # extract percentage from   ... 34% ... or ... 34 % ...
	my $p = $1;
	if (!defined $p) {
		AI_logMessage ("Failed to read disk usage percentage", 302033, $ai_prid,0);
		return "";
	}
#	dbgprint "percent usage in $dir is $p\n";
	return $p;
}

sub ExceededOutputUsage {
	my $p = PercentUsage($ai_localDIR[0]);
	if (!$p) {
		return 0;
	}
	return 1 if ($p > $ai_maxOutputFS);
	return 0;
}

sub NormalOutputUsage {
	my $p = PercentUsage($ai_localDIR[0]);
	if (!$p) {
		return 0;
	}
	return 1 if ($p <= $ai_revertOutputFS);
	return 0;
}

sub ExceededAlternativeUsage {
	my $p = PercentUsage($ai_altDIR[0]);
	if (!$p) {
		return 0;
	}
	return 1 if ($p > $ai_maxAltFS);
	return 0;
}

sub RemoveArchiveLocks {
	AI_logMessage ("Removing locks on archive files",  302034, $ai_prid, 1);
	my ($fromfile, $tofile);
	for $fromfile (keys %archiveLocks)
	{
		$fromfile =~ /(.*)\.temp$/o;
		if ($1) {
			$tofile = $1;
			AI_logMessage ("Removing locks: rename $fromfile to $tofile",  302035, $ai_prid, 0);
			move($fromfile, $tofile);
		}
	}
	%archiveLocks = (); # completely empty the list of locks
}

sub CreateTarPath {
	my $archiveInfo = shift; # reference to array of references to hashes...
	my $archiveLocation = $archiveInfo->[0]; # reference to hash of archive locations
	my $archiveFiles = $archiveInfo->[1];    # reference to hash of archive current internal file counts
	my $archiveSize = $archiveInfo->[2];     # reference to hash of archive current size
	my $basedir = shift;
	my $match = shift;
	my $tarcount = $archiveInfo->[3]->{$match}; # current archive index for this match
# $tarcount = sprintf("%04d", $tarcount); ??

	my $path;
# create .tar.temp files to start with, will rename on exit
	$path = File::Spec->catfile($basedir, "${ai_remoteHostReplaced}${match}_${ai_timestamp}_${tarcount}.tar.temp");
#	dbgprint "createtarpath $basedir, $match, $ai_timestamp, $tarcount -> $path\n";
	$archiveLocation->{$match} = $path;
	$archiveFiles->{$match} = 0;
	$archiveSize->{$match} = 0;
	return $path;
}

sub CreateAppendTar {
	my $cmd = shift;
	my $tarfile = shift;
	my $subfile = shift;

	my $filedir = dirname($subfile);
	$subfile = basename($subfile);

	AI_logMessage ("Archiving [$cmd] file $subfile to $tarfile", 302036, $ai_prid,0);
	my $arch = "$ai_Tar ${cmd}f '$tarfile' -C '$filedir' '$subfile'";
	system $arch;
	AI_logMessage ("Archive command $arch failed", 302037, $ai_prid, 4) if ($? >> 8);

	$archiveLocks{$tarfile}=1 if ($cmd eq "c"); # expect $tarfile is xxx.tar.temp, make a record to rename later to xxx.tar
}

sub CreateTar {
	CreateAppendTar("c", @_);
}

sub AppendTar {
	CreateAppendTar("r", @_);
}

sub processSubDir
{
	## 3.0-4 datetimeNow set per directory
	if ($ai_dateCheck) {
		$ai_datetimeNow = time() - ($ai_FTPTimeOffset*3600);
	}

	my ($subdir_level, $subdir_prepend, $subdir_append, $newDirectory, $enteringDatedDir) = @_;
	if ($$ == $mainPID && $subdir_level == $fork_level)
	{
		AI_logMessage("Main PID $$, processSubDir [$subprocs] $subdir_level, $newDirectory", 302038, $ai_prid, 0);
		if ($subprocs == $max_subprocs)
		{
			AI_logMessage("Reached maximum number of subprocesses $max_subprocs, waiting for a child to finish", 302039, $ai_prid, 0);
			while ($subprocs == $max_subprocs)
			{
				my $res = wait;
				AI_logMessage("Finished wait ($res)", 302040, $ai_prid, 0);
				if ($res == -1) {
					AI_logMessage("*** Unexpected return value from wait ***", 302041, $ai_prid, 4);
				}
				if (defined $connectionMap{$res})
				{
					# one of ours...
					AI_releaseConnection($connectionMap{$res}) if ($parentOwnsConnections);
					delete $connectionMap{$res};
					$subprocs--;
				} else {
					AI_logMessage("---- not our child?? $res", 302042, $ai_prid, 0);
				}
			}
		}

	# from CMB safety... this should be impossible...
		if ($$ != $mainPID)
		{
			AI_logMessage("**** Failed PID check, this pid=$$, main pid=$mainPID, exiting ****", 302043, $ai_prid, 5);
			exit 999;
		}

		AI_heartBeat($ai_prid, $ai_defaultProcDirectory) if ($ai_useMonitorFile);

		if ($parentOwnsConnections)
		{
			$oldhandle = $ai_ftpHandle;
			$connectionNumber = AI_getConnection();
		} else {
			$connectionNumber = 0;
		}

		$fpid = fork();
		if ($fpid==0)
		{
			## child
			AI_logMessage("New Child PID $$ [$subprocs], processSubDir $subdir_level, $newDirectory", 302044, $ai_prid, 0);
			########## for log debugging ## $prid .= "_$$";
			if (!$parentOwnsConnections)
			{
				AI_getConnection();
			}

			if (ftp_cd($newDirectory))
			{
				processdir($subdir_level, $subdir_prepend, $subdir_append, $newDirectory, $enteringDatedDir);
				AI_logMessage("Child process $$ completed, exiting", 302045, $ai_prid, 0);
			} else {
				AI_logMessage("Failed to cd to $newDirectory [$$]", 302046, $ai_prid, 4);
			}


			closeHandles("dlq");

			exit 0;
		} else {
	## parent
			AI_logMessage("parent $$ of $fpid", 302047, $ai_prid, 0);
			$connectionMap{$fpid} = $connectionNumber;
			$ai_ftpHandle = $oldhandle if ($parentOwnsConnections);
			$subprocs++;
		  #      push @children, $fpid;
		}
 	} else {
		if ($$ == $mainPID)
		{
			AI_logMessage("Main PID $$ not going to fork [subprocs == $subprocs], processSubDir $subdir_level, $newDirectory", 302048, $ai_prid, 0);
		} else {
			AI_logMessage("Existing Child PID $$ [subprocs == $subprocs], processSubDir $subdir_level, $newDirectory", 302049, $ai_prid, 0);
			# we are already a child process - not going to fork
		}
		if (ftp_cd($newDirectory))
		{
			processdir($subdir_level, $subdir_prepend, $subdir_append, $newDirectory, $enteringDatedDir);
		} else {
			AI_logMessage("Failed to cd to $newDirectory [$$]", 302050, $ai_prid, 4);
		}
	}    
}

#######
####### sub processdir
#######

sub processdir {
    #  Arguments:
  # subdir_level, subdir_prepend, subdir_append, input directory, wearedateddir
##    #  1. Input Directory
##    #  2. WeAreDatedDir

    my $subdir_level = shift;
    my $subdir_prepend = shift;
    my $subdir_append = shift;
    my $inDir = shift;
    my $weAreDatedDir = shift;
	
    my @directoryListing = ();

    if ($ai_sigint) {
        return;
    } ## 3.4-4
	if(!$ai_upload) {
		print ("Processing Remote Directory: $inDir \n") if ($ai_verbose);
	} else {
		print ("Processing Local Directory: $inDir \n") if ($ai_verbose);
	}
    $subdir_level++;

	dbgprint "processdir $$ handle=$ai_ftpHandle [$ai_currentConnection] going to ls $inDir\n";

    if ($ai_SFTP==3) {
		# external SFTP
		if(!$ai_upload) {
			extSFTP_ls(\@directoryListing, $inDir);
		} else {
    		if($inDir =~ /\\/o) { # check only if WIN
				$inDir =~ s/\//\\/go; # for Win format. It does not like \ and /
			}
			@directoryListing = `$ai_uploadFileList $inDir`;
			if ($ai_verbose) {
				print "Files in source directory local: \n";
				foreach $listentry (@directoryListing) {
					print "$listentry";
				}
			}
		}
#      dbgprint "*** external sftp ls\n   $directoryListing[0]\n   $directoryListing[1]\n***\n";
    }
    elsif ($ai_SFTP==1 or $ai_SFTP==2) { # SFTP
#eval
#{
        if(!$ai_upload) {
			push @directoryListing, $ai_ftpHandle->ls($inDir); # push() function is used to push a value or values onto the end of an array
			if ($ai_verbose) {
				dbgprint "SFTP Files in remote directory:\n";
				print "SFTP Files in remote directory:\n";
				foreach $hashref (@directoryListing) {
					print "$$hashref{'longname'}\n";
					dbgprint "ls: $$hashref{'longname'}\n";
				}
			}
        } else {
    		if($inDir =~ /\\/o) { # check only if WIN
				$inDir =~ s/\//\\/go; # for Win format. It does not like \ and /
			}
			@directoryListing = `$ai_uploadFileList $inDir`;
			if ($ai_verbose) {
				print "Files in source directory local: \n";
				foreach $listentry (@directoryListing) {
					print "$listentry";
				}
			}
        }
#};
#if ($@) {dbgprint "error $@"} else {dbgprint "not an error $@"}
    } elsif ($ai_localFile) { # Local
        @directoryListing = `$ai_localFileList $inDir`;
        if ($ai_verbose) {
            print "Files in source directory:\n";
            foreach $listentry (@directoryListing) {
				print "$listentry\n";
            }
        }
    } else { # FTP
		if(!$ai_upload) {
			@directoryListing =  $ai_ftpHandle->dir;
			if ($ai_verbose) {
				print "FTP Files in remote directory:\n";
				foreach $listentry (@directoryListing) {
					print "$listentry\n";
				}
			}
		} else { # upload
			if($inDir =~ /\\/o) { # check only if WIN
				$inDir =~ s/\//\\/go; # for Win format. It does not like \ and /
			}
			@directoryListing = `$ai_uploadFileList $inDir`;
			if ($ai_verbose) {
				print "Files in source directory local: \n";
				foreach $listentry (@directoryListing) {
					print "$listentry";
				}
			}
		}
    }

    my $entry;
    my $allowMultiLine = 0;
    my $awaitInitialDirectory = 0;

    if ($ai_vmsStyle) { #### was ($FTPStylePos{'Name'} == 0) {
        dbgprint " VMS-Style Allowing Multi-Line\n" if ($ai_verbose);
        $allowMultiLine = 1; # typically VMS-Style, allow to break lines after a long filename in first field
        $awaitInitialDirectory = 1;
    }

    my $multiLine = 0;
    my $fileName;
    foreach $listEntry (@directoryListing) {
        # print "listEntry=$listEntry\n";
## dbgprint "listEntry=$$listEntry{'longname'}";
        my $enteringDatedDir=0;

        if ($ai_SFTP==3) { # external SFTP
#          dbgprint "*** external sftp extract ls -l line ***\n";
			$entry = $listEntry; 
			chomp $entry;
        } elsif ($ai_SFTP==1 or $ai_SFTP==2) { # SFTP
			if(!$ai_upload) {
				$entry = $$listEntry{'longname'};
			} else {
				$entry=$listEntry;
				chomp $entry;				
			}
        } else { # FTP, Local
            $entry=$listEntry;
            chomp $entry;
        }
		# Heartbeat update pid
		AI_heartBeat($ai_prid, $ai_defaultProcDirectory) if ($ai_useMonitorFile);
		if ($ai_sigint) {
			return;
		} ## 3.4-4
		if ($awaitInitialDirectory && $entry =~ /Directory.*\]$/io) { # ignore initial "Directory... [...]" line (VMS-style)
			print ("Ignoring Directory line: $entry\n") if ($ai_verbose);
			$awaitInitialDirectory = 0;
			next;
		}
		if ( ($entry =~ /^total/o)  # unix-style line to be ignored
				|| ($entry =~ /Total of/o) # VMS-style line to be ignored
			) {
			print ("Ignoring line: $entry\n") if ($ai_verbose);
			next;
		}
		if ($entry =~ /^\s*$/o) { # ignore blank lines (eg VMS-style)
			print ("Ignoring blank line\n") if ($ai_verbose);
			next;
		}
		if (($entry =~ /Volume in/o) || ($entry =~ /Volume Serial/o) ||
        ($entry =~ /Directory of/o) || ($entry =~ /File(s)/o) ||
        ($entry =~ /Dir(s)/o)) { # ignore lines from WIN style
			print ("Ignoring line: $entry\n") if ($ai_verbose);
			next;
		}

		print ("Processing File: $entry\n") if ($ai_verbose);
		my @listValues;
		my $perm;
		$perm="";
		my $fileSize=0;

		@listValues = split /\s+/ ,$entry, $ai_StyleNoFields;

	#	for ($ii=0; $ii<@listValues; ++$ii) {print "listval[$ii]: $listValues[$ii]\n";}

		if (!$allowMultiLine && ((scalar @listValues) < $ai_StyleNoFields)) {
			print "Ignoring line with too few fields: $entry\n" if ($ai_verbose);
			next;
		}

		## 3.0-2
		if (!$multiline) {
			$fileName = $listValues[$FTPStylePos{'Name'}];

			if ($allowMultiLine && (scalar @listValues) == 1) {
				dbgprint " Detected split line\n";
				$multiline = 1;
				next;
			}
		} else {
			# multiline: Second line corresponding to a single file, $fileName is already set
			$multiline = 0; # reset
			# first field is '' (line starts with whitespace), rest are as they should be, proceed
		}

		$fileSize = $listValues[$FTPStylePos{'Size'}] if ($ai_CheckingFileSize);
		# print ("fileSize = '$fileSize'\n") if ($ai_verbose);
		if (exists $FTPStylePos{'Dir'}) {
			$perm = $listValues[$FTPStylePos{'Dir'}];
		}

		if ($perm eq "") { ## No directory match
			if ($ai_vmsStyle && $fileName =~ /.DIR;/io) {
				# detected VMS directory, strip .DIR;* so we can do cd dirname
				$perm = $defaultFTPDirMatch;
				$fileName =~ s/.DIR;[0-9]*$//io;
				dbgprint " stripped .DIR;* from VMS directory, now $fileName\n";
			} elsif ($fileName =~ /$ai_fileMask/o) {
				$perm = "-rwx";
				print ("$fileName identified as file \n") if ($ai_verbose);
			} elsif (!$ai_vmsStyle && $fileName =~ /$ai_dirMask/o) {
				$perm = $defaultFTPDirMatch;
				print ("$fileName identified as directory \n") if ($ai_verbose);
			} else {
				$perm = "";
				print ("$fileName not identified\n") if ($ai_verbose);
				###      dbgprint " $fileName not identified - but going to treat it as a file...\n"
				# may as well do filemask failure here...
			}
		}

		if ($perm =~ /^$ai_FTPDirMatch/ ) {
			if (!($fileName =~ /^\./o )) {  #Exclude special directories starting with .
				###
				### Found a Directory
				###
		
				#############
				# just for debugging?
				if ($ai_verbose) {
					my $fileDate;
					my $datMask = dateMask($ai_dateFormat);
					if ($fileName =~ /($datMask)/o) {
						$fileDate=$1;
						print "Checking datedDir: $fileName => $fileDate ($ai_dateFormat-$datMask) :: $lastFileList{$fileDate}\n" if ($ai_verbose);
					}
				}
				#############
		
				my $newDirectory;
				#datedDir means test vs dateformat for the "dated directory" at the specified level
				if ($ai_datedDir == $subdir_level) {
					#    dbgprint " match subdir level $ai_datedDir\n";
					my $gfd=getFileDate($fileName);
					if ("$gfd" ne "") { ## 3.0-6, 3.2-1, 3.5, 3.6-3
						$ai_useDate=$gfd;
						#        dbgprint " set ai_useDate=$ai_useDate\n";
						$newDirectory = "$inDir/$fileName";
						$enteringDatedDir=1;
					} else {
						#        dbgprint " UNSET ai_useDate=$ai_useDate\n";
						next;
					}
				} elsif ($fileName =~ /$ai_dirMask/) {
					#    dbgprint " matched on dirmask\n";
					$newDirectory = "$inDir"."/$fileName";
				} else {
					next;
				}
		
				#Recurse sub-directories
		
				my $ppsd; my $old_subdir_prepend = $subdir_prepend;
				foreach $ppsd (@ai_PrependSubDir) {
					if ($ppsd == $subdir_level) {
						if ($ai_PrependSubStr and $fileName =~ /($ai_PrependSubStr)/) {
							$subdir_prepend .= $1.$ai_PrependSeparator;
						} else {
							$subdir_prepend .= $fileName.$ai_PrependSeparator;
						}
					}
				}
				my $asd; my $old_subdir_append = $subdir_append; # v3.17
				foreach $asd (@ai_AppendSubDir) # v3.17
				{
					if ($asd == $subdir_level) {
						if ($ai_AppendSubStr and $fileName =~ /($ai_AppendSubStr)/) {
							$subdir_append .= $ai_AppendSeparator.$1;
						} else {
							$subdir_append .= $ai_AppendSeparator.$fileName;
						}
					}
				}
		
				if ($max_subprocs > 0)
				{
					processSubDir($subdir_level, $subdir_prepend, $subdir_append, $newDirectory, $enteringDatedDir);
				} else {
					if (ftp_cd($newDirectory)) {
						## 3.0-4 datetimeNow set per directory
						if ($ai_dateCheck) {
							$ai_datetimeNow = time() - ($ai_FTPTimeOffset*3600);
						}
						processdir($subdir_level, $subdir_prepend, $subdir_append, $newDirectory, $enteringDatedDir);
					} else {
						AI_logMessage("Failed to cd to subdirectory: $newDirectory", 302051, $ai_prid, 3);
					}
				}
		
				foreach $ppsd (@ai_PrependSubDir) {
					if ($ppsd == $subdir_level) {
						$subdir_prepend=$old_subdir_prepend;
					}
				}
				foreach $asd (@ai_AppendSubDir) # v3.17
				{
					if ($asd == $subdir_level) {
						$subdir_append=$old_subdir_append;
					}
				} # v3.17
		
				#reset old directory
		
				if (!ftp_cd($inDir)) {
					## TODO Consider this as a log error and exit??
					return;
				}
			}
		} else {
			###
			### Found a File
			###
			my $fullFileName = $fileName; my $tempBefore="";
			my $fullFileNamePlusOne = "";
			my $fullFileNameMinusOne = "";
			my $fileNameForListFile;
			if ($subdir_prepend ne "") {
				$fullFileName = $subdir_prepend.$fileName;
			}
			if ($subdir_append ne "") # v3.17
			{
				if( $ai_AppendBefore and $fileName =~ /($ai_AppendBefore)/ ) # v3.17
				{
					$tempBefore .= substr( $fileName, 0, -length($ai_AppendBefore) ); # v3.17
					$tempBefore .= $subdir_append; # v3.17
					$fullFileName = $tempBefore.$ai_AppendBefore; # v3.17
				} else # v3.17
				{
					$fullFileName = $fileName.$subdir_append;
				} # v3.17
			}

			dbgprint "BEFORE replace - fullFileName: $fullFileName ,,,, fileName: $fileName ,,,, ai_ReplacePoundWith: $ai_ReplacePoundWith";
			
			$fullFileName =~ s/:/$ai_ReplaceColonWith/g; ##3.0-1
			$fullFileName =~ s/#/$ai_ReplacePoundWith/g; ## v6.2.0.25
			$fullFileName =~ s/$ai_RemoveFromFileName//g if defined $ai_RemoveFromFileName; ## v6.2.0.25
			$fullFileName =~ s/\s+//go; #3.0-3
			
			dbgprint "AFTER replace - fullFileName: $fullFileName ,,,, fileName: $fileName";
			
			## 3.5-6
			if(!$ai_upload) {
				print "Downloading file: Is PrependTimestamp Set: $ai_PrependTimestamp\n" if ($ai_verbose);
			} else {
				print "Uploading file: Is PrependTimestamp Set: $ai_PrependTimestamp\n" if ($ai_verbose);
			}

			$fileNameForListFile = $fullFileName;

			if ($ai_PrependTimestamp >= 1) {
				$ftpdate="";
				$datec=0;
				foreach (@DateList) {
					if ($ftpdate ne "") {
						$ftpdate .= " ";
					}
					$ftpdate .= $listValues[$FTPStylePos{'Date'}+$datec];
					$datec++;
				}
				#$fullFileName = getDateInFormat($ftpdate,$listValues[$FTPStylePos{'Time'}]).$ai_PrependSeparator.$fullFileName; # original

				my $dateTimeForFile = getDateInFormat($ftpdate,$listValues[$FTPStylePos{'Time'}]);
				my $dateTimePlusOne = $dateTimeForFile + 3600;
				my $dateTimeMinusOne = $dateTimeForFile - 3600;
				my $origFullFileName = $fullFileName;
				$fullFileName = $dateTimeForFile.$ai_PrependSeparator.$origFullFileName;

				# WindowsDST fix operational implies PrependTimestamp == 1
				$fullFileNamePlusOne = $dateTimePlusOne.$ai_PrependSeparator.$origFullFileName;
				$fullFileNameMinusOne = $dateTimeMinusOne.$ai_PrependSeparator.$origFullFileName;

				$fileNameForListFile = $fullFileName if ($ai_PrependTimestamp != 2);
				# get these ready to pass through ftp_file, to store them if we download a new file
			}

			if ($ai_NoteFileSize) {
				$fileNameForListFile = "${fileSize}_$fileNameForListFile";
				$fullFileNamePlusOne = "${fileSize}_$fullFileNamePlusOne";
				$fullFileNameMinusOne = "${fileSize}_$fullFileNameMinusOne";
			}

			##--
			$fullFileName =~ s/$ai_zipExtension$// if ($ai_removeZipExt); ## 3.6-1

			print ("Checking file: $fileName - ($fullFileName).\n") if ($ai_verbose);
			AI_logMessage ("Checking file: $fullFileName", 302052, $ai_prid,0);
			#if ($fileSize > 0 and (!$ai_MAXFileSize or $ai_MAXFileSize > $fileSize)) {  ## 3.3-3 Check if filesize is less than MAXfileSize ## 3.4-2 filesize is greater that 0
			if (!$ai_CheckingFileSize || ($fileSize > 0 and (!$ai_MAXFileSize or $ai_MAXFileSize > $fileSize) and
					(!$ai_MINFileSize or $ai_MINFileSize < $fileSize) ))
			{  ## 3.3-3 Check if filesize is less than MAXfileSize ## 3.4-2 filesize is greater that 0
				if ($fileName =~ /$ai_fileMask/) { ## Check if fileMask is okay
					if (not $fileName =~ /$ai_excludeMask/) { ##3.2-5
					# if ($ai_dateFormat eq "0" or $ai_datedDir or ($ai_useDate=getFileDate($fileName)) ne "") { ## Check if date is in filename if ai_datedDir=0
						if ($ai_dateFormat eq "0" or ($ai_datedDir && $weAreDatedDir) or
							($ai_useDate=getFileDate($fileName)) ne "") { ## Check if date is in filename if ai_datedDir=0

							# if $ai_dateFormat is not "0", then $ai_usedate must be set at this point, either
							# by having entered a valid dated dir, or from getfiledate (if not using datedDir)
						  
						    my $fileFound = 0;
							
							if ($ai_WindowsDSTFix == 0) {
							    $fileFound = filesearch($fileNameForListFile);
							} else {
 							    # check the filename (prepended) vs stored filenames (prepended +- 1 hour)
							    if (FileSearchForWin("${inDir}|$fileNameForListFile", \%lastFileList) ||
								FileSearchForWin("${inDir}|$fileNameForListFile", \%lastFileListPlusOneH) ||
								FileSearchForWin("${inDir}|$fileNameForListFile", \%lastFileListMinusOneH)) {
									$fileFound = 1;
							    }
							}
#							dbgprint "fileFound = $fileFound\n";
							if (!$fileFound) {    ## Check if file has been downloaded already

						# move safetyperiod check inside ftp_file, so we can defer
						# it if using multiple processes (as it writes to a hash, so needs
						# to be in main process)

								$fullFileName .= $ai_zipExtension if ($ai_removeZipExt); ##3.6-1

								if(!$ai_upload)
								{
									#ftp_file($fileName,$fileSize,$fullFileName,$inDir,$ai_useDate, "", 1);
									ftp_file($fileName,$fileSize,$fullFileName,$inDir,$ai_useDate, "", 1, $fullFileNamePlusOne, $fullFileNameMinusOne, $fileNameForListFile); 
								} else {
									dbgprint ("fileName: $fileName - fileSize: $fileSize - fullFileName: $fullFileName - inDir: $inDir - ai_useDate: $ai_useDate --- ai_localDIR[0]: $ai_localDIR[0]\n");
									ftp_file($fileName,$fileSize,$fullFileName,$inDir,$ai_useDate, $ai_localDIR[0], 1, $fullFileNamePlusOne, $fullFileNameMinusOne, $fileNameForListFile);
								}

		#                        if ($ai_FTPSafetyPeriod) {
		#                            # safetyperiod > 0, check that we first saw this file at least $ai_FTPSafetyPeriod mins ago
		#                            $scFileName = $fullFileName;
		#                            if (!safetyCheck($scFileName, $fileSize)) {
		#                                AI_logMessage ("$fileName: In Safety Period ($ai_FTPSafetyPeriod mins) - do not download", 3020xx, $ai_prid,1);
		#                                next;
		#                            }
		#                        }
		#                        print "need to download file\n" if ($ai_verbose);
		# ftp_file()
							} else {
								print "Already downloaded\n" if ($ai_verbose);
							}
						} else {
							print "dates ($dateList) not in filename\n" if ($ai_verbose);
						}
					} else {
							print "excludeMask match - file excluded\n" if ($ai_verbose);
						} ##3.2-5
				} else {
					print "FileMask failure\n" if ($ai_verbose);
				}
			} #else { print "File is too big: $fileSize > $ai_MAXFileSize\n" if ($ai_verbose); }
			else {
				AI_logMessage ("$fileName - File is NOT within the size range $ai_MINFileSize < $fileSize < $ai_MAXFileSize therefore not downloaded.", 302053, $ai_prid,0);
			}
		} # found a file
	} # foreach $listEntry (@directoryListing)
    $subdir_level--;
} ### END sub processdir


## 3.2-1 sub getFileDate
sub getFileDate {
    my ($fileName) = @_;
    foreach $key (sort keys %lastFileList) {
        if ($fileName =~ /$key/) {
            return $key;
        }
    }
    return "";
}

sub testDirEmpty {
# improve this once we understand the semantics

    my $inDir = shift(@_);
    my $d = new DirHandle "$inDir";
    my $infile;
    if (defined $d) {
        while (defined($filename = $d->read))
        {
            $infile = "$inDir"."/$filename";
            if (-f $infile) {
				return 0;
           }
        }
        undef $d;
    }
    return 1;
}

sub getFileCount {
    my $inDir = shift(@_);
    my $d = new DirHandle "$inDir";
    my $filecount=0;
    my $infile;
    if (defined $d) {
        while (defined($filename = $d->read))
        {
            $infile = "$inDir"."/$filename";
            if (-f $infile) {
                $filecount++;
           }
        }
        undef $d;
    }
    return $filecount;
}

sub getDirCount {
    my $inDir = shift(@_);
    my $d = new DirHandle "$inDir";
    my $dircount=0;
##    my $infile;

    if (defined $d) {
        ## Foreach file in REPORT directory
        while (defined($filename = $d->read)) {
            ##my $pathFilename = $indir.$ds.$filename;
            my $pathFilename = $inDir."/".$filename;

            if ( $filename =~ /^\./o || !(-d $pathFilename) ) {
                next;
            } #Skip special/hidden directories && directories
            else{
                print "pathFilename: $pathFilename  --  filename: $filename \n";
                $dircount++;
            }
        }
        undef $d;
    }

    return $dircount;
}

sub scWriteOut {
# serialize safetyCheck data out to file
    my $OUTFILE;
    $serialFile = "${ai_listDirectory}/safetyCheckList_$ai_prid";
    dbgprint " scWriteOut $serialFile\n";

    if(!(open ($OUTFILE, ">$serialFile"))) {
        AI_logMessage ("Couldn\'t open file $serialFile for output", 302054, $ai_prid, 4);
        return;
    }

    for $f (keys %fileTimes) {
        print $OUTFILE "$f\n";
        print $OUTFILE "$fileTimes{$f} ";
        if ($fileSizes{$f}) {
            print $OUTFILE "$fileSizes{$f}\n";
        } else {
            print $OUTFILE "-1\n";
        }
    }
    close $OUTFILE;
}

sub scReadIn {
    # serialize safetyCheck data in from file
    my $INFILE;
    $serialFile = "${ai_listDirectory}/safetyCheckList_$ai_prid";

    dbgprint " scReadIn $serialFile\n";
    if(!(open ($INFILE, "<$serialFile"))) {
        AI_logMessage ("Couldn\'t open file $serialFile for reading", 302055, $ai_prid,4);
        return;
    }

    undef %fileTimes;
    undef %fileSizes;

    while (<$INFILE>) {
        chomp;
        $f = $_;
        last if (!($_=<$INFILE>));
        chomp;
        ($t,$s) = split;
        $fileTimes{$f} = $t;
        $fileSizes{$f} = $s if ($s != -1);
        dbgprint " scReadIn $f: $t,$s\n";
    }
    close $INFILE;
}

sub safetyCheck {
    my $filename = shift;
    my $filesize = shift;

    $oldTime = $fileTimes{$filename};
    $newTime = time();

    if (!defined $oldTime) {
        # first time we've seen this file - store time and fail safety check
        $fileTimes{$filename} = $newTime;
        $fileSizes{$filename} = $filesize if ($ai_CheckingFileSize);
        AI_logMessage ("$filename: First discovery, cannot be sure that it is complete - do not download", 302056, $ai_prid,1);
        return 0;
    }

    if ($ai_CheckingFileSize) {
        if ($fileSizes{$filename} != $filesize) {
            # extra check - looks like xfer still/recently in progress, bump the stored time
            $fileTimes{$filename} = $newTime;
            $fileSizes{$filename} = $filesize;
        }
    }

    if ($newTime - $oldTime > $ai_FTPSafetyPeriod * 60) {
        # that's long enough ago, pass
        return 1;
    }

    # fail check
    AI_logMessage ("$filename: Safety period (".$ai_FTPSafetyPeriod." min) has not passed - do not download", 302057, $ai_prid,1);
    return 0;
}

sub getDateInFormat {
    my ($ftpdate,$ftptime) = @_;
    my ($year,$month,$day);
    my $usingDefaultYear = 0;

    print "GetDateInFormat: $ftpdate($ai_FTPDateFormat),$ftptime($ai_FTPTimeFormat)\n" if ($ai_verbose);

    $mask=dateMask($ai_FTPDateFormat,'Y');
    if ($ftpdate =~ /$mask/) {
        $year=$1;
    }
    $mask=dateMask($ai_FTPDateFormat,'M');
    if ($ftpdate =~ /$mask/) {
        $month=$1;
    }
    $mask=dateMask($ai_FTPDateFormat,'D');
    if ($ftpdate =~ /$mask/) {
        $day=$1;
    }

    if ( !defined $year && ( ($ai_FTPDateFormat =~ /YYYY/o) || ($ai_FTPDateFormat =~ /YY/o) ) ) {
        # in these cases, we need to match the year to proceed
        AI_logMessage ("Did not match year from '$ftpdate', please check FTPDateFormat ('$ai_FTPDateFormat')", 302058, $ai_prid,2);
        return time();
    }

    if ( !defined $month ) {
        AI_logMessage ("Did not match month from '$ftpdate', please check FTPDateFormat ('$ai_FTPDateFormat')", 302059, $ai_prid,2);
        return time();
    }

    if ( !defined $day ) {
        AI_logMessage ("Did not match day from '$ftpdate', please check FTPDateFormat ('$ai_FTPDateFormat')", 302060, $ai_prid,2);
        return time();
    }

    if ($ai_FTPDateFormat =~ /YYYY/o) {
        $year -= 1900;
    } elsif ($ai_FTPDateFormat =~ /YY/o) {
        if ($year < 70) {
            $year += 100;
        }
    }
    else {
        $usingDefaultYear = 1;
		($year)=(localtime)[5];
    }

    ## dbgprint " GetDateInFormat: year: $year, month: $month, day: $day\n";
    ## dbgprint " GetDateInFormat: $ftpdate($ai_FTPDateFormat),$ftptime($ai_FTPTimeFormat)\n";

    if ($ai_FTPDateFormat =~ /MTH([0-9]+)/o) {
        $mthlen=$1; my ($mth, $count); $count=0;
        if ($mthlen == 2)# || $mthlen == 4 )
        {
            foreach $mth (@MTH2) {
                if (uc($mth) eq $month) {
                    $month=$count;
                    last;
                }
                $count++;
            }
        } else {
            foreach $mth (@MONTHS) {
                if (uc(substr($mth,0,$mthlen)) eq $month) {
                    $month=$count;
                    ##     dbgprint " got mth index=$count\n";
                    last;
                }
            $count++; }
        }
    } elsif ($ai_FTPDateFormat =~ /Mth([0-9]+)/o) {
        $mthlen=$1; my ($mth, $count); $count=0;
        if ($mthlen == 2)# || $mthlen == 4 )
        {
            foreach $mth (@MTH2) {
                if ($mth eq $month) {
                    $month=$count;
                    last;
                }
            $count++;
            }
        } else {
            foreach $mth (@MONTHS) {
                if (substr($mth,0,$mthlen) eq $month) {
                    $month=$count;
                    ##     dbgprint " got mth index = $count\n";
                    last;
                }
                $count++;
            }
        }
    } else {
        ##     dbgprint " doing default month from $month\n";
        $month-=1;
    }

    # change from >12 to >=12: timelocal expects month in 0..11
    if ($month < 0 || $month > 11) {
        AI_logMessage ("Month $month out of range please check FTPDateFormat", 302061, $ai_prid,1);
        return time();
    }

    if ($day < 1 || $day > 31) {
        AI_logMessage ("Day $day out of range please check FTPDateFormat", 302062, $ai_prid,1);
        return time();
    }

	if ($usingDefaultYear == 1 && $ai_FTPTimeFormat =~ /:/o && $ftptime =~ /^[0-9]{4}$/o ) {
		# we were going to guess the year, we were looking for something like HH:MM in the time field, ...
		# but the time field turned out like YYYY
		# (looks like a file more than 1 year old)
		$year = $ftptime;
		$usingDefaultYear = 0;
	}
	
	my $datetime = timelocal(0,0,0,$day,$month,$year);
	dbgprint " day= $day, month= $month, year= $year, datetime= $datetime\n";
	
	if ($usingDefaultYear) {
		# OB-1750 - We had to guess the year - if we end up in the future (taking into account
		# timezones + system clock skew), we should have guessed last year.
		my $slop = 2 * 60*60*24; # 2 days should be plenty
		if ($datetime > time() + $slop) {
			$datetime = timelocal(0,0,0,$day,$month,$year-1);
			print "Assuming date ($day, $month) belongs to last year\n" if ($ai_verbose);
		}
	}
	
	my $filetime = getTimeInMinutes($ftptime)*60;
	my $totTime = $datetime + $filetime;
	dbgprint " ftptime = $ftptime, filetime=$filetime,  datetime+filetime= $totTime\n";
    	
    return $totTime;
}

sub getTimeInMinutes {
    my ($ftptime) = @_;
    my ($hourMask,$hour,$ampm,$min);
    $hour = 0; $min = 0;

    $hourMask=dateMask($ai_FTPTimeFormat,'H');
    my $minMask=dateMask($ai_FTPTimeFormat,'MI');
    if ($ai_FTPTimeFormat =~ /[AP]M/o) {
        if (($ai_FTPTimeFormat =~ /AM/o and index($ai_FTPTimeFormat,"AM") < index($ai_FTPTimeFormat,'HH')) or
            ($ai_FTPTimeFormat =~ /PM/o and index($ai_FTPTimeFormat,"PM") < index($ai_FTPTimeFormat,'HH'))) {
            if ($ftptime =~ /$hourMask/o) {
                $hour=$2; $ampm=$1;
            }
        } else {
            if ($ftptime =~ /$hourMask/o) {
                $hour=$1; $ampm=$2;
            }
        }
        if ($hour == 12) {
            if ($ampm eq "AM") {
                $hour = 0;
            }
        } elsif ($ampm eq "PM") {
            $hour += 12;
        }
    } else {
        if ($ftptime =~ /$hourMask/o) {
            $hour=$1;
        }
    }
    if ($ftptime =~ /$minMask/o) {
        $min=$1;
    }
    return ($hour*60 + $min);
}

sub dateMask {
    my ($dateformat,$collect) = @_;
    if (!defined $collect) {
        $collect="";
    }
    if ($collect eq "M") {
        $dateformat =~ s/MM/([0-9]{2})/o;
        $dateformat =~ s/M[tT][hH]([0-9]+)/(\\w{$1})/o;
    } else {
        $dateformat =~ s/MM/[0-9]{2}/o;
        $dateformat =~ s/M[tT][hH]([0-9]+)/\\w{$1}/o;
    }
    if ($collect eq "Y") {
        $dateformat =~ s/YYYY/([0-9]{4})/o;
        $dateformat =~ s/YY/([0-9]{2})/o;
    } else {
        $dateformat =~ s/YYYY/[0-9]{4}/o;
        $dateformat =~ s/YY/[0-9]{2}/o;
    }
    if ($collect eq "D") {
        $dateformat =~ s/DD/([0-9]{2})/o;
        $dateformat =~ s/D/(\\s?[0-9]{1,2})/o;
    } else {
        $dateformat =~ s/DD/[0-9]{2}/o;
        $dateformat =~ s/D/\\s?[0-9]{1,2}/o;
    }
    if ($collect eq "H") {
        $dateformat =~ s/HH24/([0-9]{2})/o;
        $dateformat =~ s/HH/([0-9]{2})/o;
        $dateformat =~ s/[AP]M/([AP]M)/o;
    } else {
        $dateformat =~ s/HH24/[0-9]{2}/o;
        $dateformat =~ s/HH/[0-9]{2}/o;
        $dateformat =~ s/[AP]M/[AP]M/o;
    }
    if ($collect eq "MI") {
        $dateformat =~ s/MI/([0-9]{2})/o;
    } else {
        $dateformat =~ s/MI/[0-9]{2}/o;
    }
    $dateformat =~ s/SS/[0-9]{2}/o;
    $dateformat =~ s/\//\\\//go;
    return $dateformat;
}

#--
sub filesearch {
    #my $target = shift(@_); # Shifts the first value of the array off and returns it, shortening the array by 1 and moving everything down
	my ($target) = @_;
	if ($ai_dateFormat eq "0") {
	    return $lastFileList{$target};
	} else {
	    return $lastFileList{$ai_useDate}{$target};
	}
}

sub FileSearchForWin {
	my ($target, $lastFileListRef) = @_;
# dereferencing hash-ref: ${$lastFileListRef}{$xxx}} == $lastFileListRef->$xxx

	if ($ai_dateFormat eq "0") {
	    return ${$lastFileListRef}{$target};
	}
	else {
	    return ${$lastFileListRef}{$ai_useDate}{$target};
	}
}

####### sub DFLdateTimeFolder for v6.2.0.6
##sub DFLdateTimeFolder {
##    my($s,$mi,$h,$d,$m,$y)=(localtime)[0,1,2,3,4,5];
##    $y+=1900; $m++;
##    $m="0$m" if ($m<10);
##    $d="0$d" if ($d<10);
##    $mi="0$mi" if ($mi<10);
##    $h="0$h" if ($h<10);
##    $s="0$s" if ($s<10);
##    ($shhh, $usec) = gettimeofday();
##    $usec = floor($usec / 1000);
##    $usec="00$usec" if ($usec<10);
##    $usec="0$usec" if ($usec<100);
##    ##$currentOutFolderName = $y.$m.$d.$h.$mi.$s.$usec;
##    ##$currentOutFolderName = $d.$h.$mi.$s.$usec;

##    return $d.$h.$mi.$s.$usec;
##}

####### sub DFLdateTimeFolder for v6.2.0.7
sub DFLdateTimeFolder {
    #my($s,$mi,$h,$d,$m,$y)=(localtime)[0,1,2,3,4,5];
	my($d,$m,$y)=(localtime)[3,4,5];
    $y+=1900; $m++;
    $m="0$m" if ($m<10);
    $d="0$d" if ($d<10);
  ##$y = sprintf("%02d", $y%100 );
  ##print "$y$m$d\n"; ## if ($ai_verbose);

    return $y.$m.$d
}

##
## DFL_GetDir
##  takes a hash reference as parameter, eg DFL_GetDir(\%backupDFL);
##
sub DFL_GetDir {
	my ($currentFolderName, $currentFolderFileCount);

	# fetch container of DFL info
	my $dflInfo = shift;

	# read current info
	my $dflRoot                = $dflInfo->{'ROOT'};
	$currentFolderName      = $dflInfo->{'FOLDER'};
	$currentFolderFileCount = $dflInfo->{'COUNT'};

 #dbgprint "DFL_GetDir current folder = $currentFolderName, root= $dflRoot\n";
 #dbgprint "DFL_GetDir check   $dflInfo->{'FOLDER'}\n";

	$bFirstFile = $dflInfo->{'FIRSTFILE'};
	if (!defined $bFirstFile) {
		# first file in this DFL, do directory maintenance, select starting folder

		$dflInfo->{'FIRSTFILE'} = 1;
		#dbgprint "DFL_GetDir first file\n";

		## get all subfolders and filter them out
		opendir(DIR, $dflRoot) || dieNicely("Can't open dir for DFL $dflRoot: $!"); ##die "Can't opendir $dflRoot: $!";
		my @arraySubDirs = grep { !/^\./ && /\_/ && -d "$dflRoot/$_" } readdir(DIR);
		closedir DIR;

		my $out_dir;
		my $size = scalar(@arraySubDirs);
		if($size>0) {
			# if there are subfolders
			# select the last non-empty subdirectory from the SORTED list

			$currentFolderName = $dflRoot;
			# directory maintenance
			for $subdir (sort @arraySubDirs) {
				$out_dir = File::Spec->catfile($dflRoot,$subdir);
				if( testDirEmpty($out_dir)) {
					rmdir ($out_dir); ## remove the empty folder
				} else {
					$currentFolderName = $out_dir;
				}
			}
		} else { # no subfolders exist
			$currentFolderName = $dflRoot; ## assign to the root
		}

#		dbgprint "DFL_GetDir first now current folder = $currentFolderName, root= $dflRoot\n";

		$currentFolderFileCount = getFileCount($currentFolderName);
	} # first file

	if ($currentFolderFileCount >= $ai_currentFolderFileLimit) {
		my $dateTime = DFLdateTimeFolder();

		### this should be if (root) {num=001} else {num=last-bit-of-current-plus-1}; newdir=rootdir/datetime_num
		### so always get current datetime (after filled up current directory, which might be old 1st time in)
		### also, should probably excluding trailing / or \ from all directories, and use File::Spec->catfile

		if( $currentFolderName eq $dflRoot ) {
			# at root, make the first subdir

			my $tempID = sprintf("%.3d", 1 ); # formatting like 001
			$currentFolderName = $dflRoot."/".$dateTime."_".$tempID."/";
		} else {
			my $uniqueNumber = 0;

			## check if last character is a / then remove it before split it
			my $var = substr($currentFolderName,length($currentFolderName)-1,1);
			if ($var eq "\\" or $var eq "/") {
				#print "found backslash as last character\n\n" if($ai_verbose);
				$currentFolderName = substr($currentFolderName, 0, -1);
			}

#			dbgprint "DFL_GetDir n current folder = $currentFolderName, root= $dflRoot\n";

			## TODO dbgprint "** basename\n";
			## split the directories from their delimiters
			my @tempArray = split( /([\/ \\])/, $currentFolderName );
			$uniqueNumber = scalar(@tempArray);
			## extract datetime and ID from the folder name /090514_001
			#my $extractedEndFolder = @tempArray[ $uniqueNumber-1 ]; ## 090513_001
			my $extractedEndFolder = $tempArray[ $uniqueNumber-1 ]; ## 090513_001

			@tempArray = split( /_/, $extractedEndFolder );

			if( $tempArray[0] == $dateTime ) {
				$tempID = $tempArray[1];
				$tempID = sprintf("%.3d", $tempID + 1);

				$currentFolderName = $dflRoot."/".$dateTime."_".$tempID."/";
			} else {
				my $tempID = sprintf("%.3d", 1 ); # formatting like 001
				# hope it doesn't already exist...
				$currentFolderName = $dflRoot."/".$dateTime."_".$tempID."/";
			}
		}
		mkdir $currentFolderName;
		$currentFolderFileCount = 0;

#		dbgprint "DFL_GetDir n+1 current folder = $currentFolderName, root= $dflRoot\n";

		##    $dflRoot = File::Spec->catfile($currentFolderName, $ai_remoteHostReplaced . $fullfilename);
		##
		##  } else
		##  {
		##    $dflRoot = File::Spec->catfile($currentFolderName, $ai_remoteHostReplaced . $fullfilename);
	}

# write back
	$dflInfo->{'FOLDER'} = $currentFolderName;
	$dflInfo->{'COUNT'}  = $currentFolderFileCount;

#	dbgprint "DFL_GetDir END current folder = $currentFolderName, root= $dflRoot\n";
#	dbgprint "DFL_GetDir END check   $dflInfo->{'FOLDER'}\n";
}

##
## archive_file
##  takes a hash reference as first parameter, eg
##    archive_file(\@altArchiveInfo, $outputDirectory, $outputDFLRef, $tempfileleaf_orig, $tempfilepath, $filesize);
##    archive_file(\@backupArchiveInfo, $currentBackupFolderName, $backupDFLRef, $tempfileleaf_orig, $tempfilepath, $filesize);
##

sub archive_file {

    my $archiveInfo = shift;
    my $outputDirectory = shift;
    my $outputDFLRef = shift;
    my $archiveFileLeaf = shift;
    my $archiveFilePath = shift;
    my $archiveFileSize = shift;

	#print "\n\n***** archive_file *****\n";
	#print "dir = $outputDirectory\n";
	#print "leaf = $archiveFileLeaf\n";
	#print "path = $archiveFilePath\n";
	#print "size = $archiveFileSize\n";
	#print "\n";

    my $archiveLocation = $archiveInfo->[0];  # reference to hash of archive locations
    my $archiveFiles = $archiveInfo->[1];     # reference to hash of archive current internal file counts
    my $archiveSize = $archiveInfo->[2];      # reference to hash of archive current size
    my $archiveTarCount = $archiveInfo->[3];  # reference to hash of archive current index (starts at 0)

    my $currentArchiveFolderName = $outputDirectory;

    my $archiveMatch;
    # NB match takes place on filename (without directory) without remote host replaced
    if ($archiveFileLeaf =~ /$archivePeriodRE/ && "$1" ne ""){
      $archiveMatch = "$1";
    } else {
      $archiveMatch = "archive"; # all in one tar file (up to limits)
    }

# dbgprint "=== match of '$archiveMatch':  $archiveFiles->{$archiveMatch}\n";

    my $archivePath = $archiveLocation->{$archiveMatch}; # set by CreateTarPath
    if (!defined ($archivePath)) {
      # this is a new group --> a new tar file
            dbgprint "=new=\n";
            $archiveTarCount->{$archiveMatch} = 0;
    }

    # we have a tar file, can we add to it, or do we need to make another one for this group?
    
    # Need to create tar (i) if new group or
    # (ii) if have exceeded max file count or file size [provided not first file in the tar, in which case ignore size]
    
    # need new tar file?
    if ( !defined $archivePath ||
	 ( ( $archiveFiles->{$archiveMatch} > 0 ) &&
	   ( $archiveFiles->{$archiveMatch} >= $ai_archiveMaxFilesInTar ||
	     $archiveSize->{$archiveMatch} + $archiveFileSize > $ai_archiveMaxSizeInTar )
	 )
       )
    {
        # new tar file
        if ($ai_UseFolderFileLimit > 0) {
	    DFL_GetDir($outputDFLRef);
	    $currentArchiveFolderName = $outputDFLRef->{'FOLDER'};
        }

	# create next tar file destination based on dir and group (match), and reset counters
	$archivePath = CreateTarPath($archiveInfo, $currentArchiveFolderName, $archiveMatch);
	CreateTar($archivePath, $archiveFilePath);
	# should check to see if already exists [unlikely due to $ai_timestamp]
	$archiveTarCount->{$archiveMatch}++;

	$outputDFLRef->{'COUNT'}++ if ($ai_UseFolderFileLimit > 0);
     } else {
         # append to existing tar file
         AppendTar($archivePath, $archiveFilePath);
     }

    $archiveFiles->{$archiveMatch}++;
    $archiveSize->{$archiveMatch} += $archiveFileSize;

# ok, have updated TarCount, Size, Files
#          dbgprint "*** path = $archivePath, f=$archiveFiles->{$archiveMatch}\n ";
          # Have appended original file to tar file, now remove original file
}




#######
####### sub ftp_file
#######

sub ftp_file {
    dbgprint "ftp_file $$ handle=$ai_ftpHandle [$ai_currentConnection]\n";

    # Downloads to a temporary directory and then moves
    my ($filename,$filesize,$fullfilename,$currentDir, $ai_useDate, $uploadDir,
		$defer, $fileNameForListFilePlusOneH, $fileNameForListFileMinusOneH, $fileNameForListFile ) = @_;
   
    dbgprint "ftp_file '$filename,$filesize,$fullfilename,$currentDir, $ai_useDate, $uploadDir, $defer, $fileNameForListFilePlusOneH, $fileNameForListFileMinusOneH, $fileNameForListFile'\n";
    dbgprint "ftp_file uploadDir: $uploadDir\n";

    if (defined $defer && $defer == 1)
    {
	my $outftpdefer;

	fetchHandle(\$outftpdefer, "dlq", ">>$ftpDownloadQueue") or dieNicely("Could not open ftpdeferlist file for appending");
	dbgprint("defer $ftpDownloadQueue $filename");
	print $outftpdefer "$filename\t$filesize\t$fullfilename\t$currentDir\t$ai_useDate\t$fileNameForListFilePlusOneH\t$fileNameForListFileMinusOneH\t$fileNameForListFile\n";

#		open OUTFTPDEFER, ">>$ftpDownloadQueue" or dieNicely("Could not open ftpdeferlist file for appending");
#		print OUTFTPDEFER "$filename\t$filesize\t$fullfilename\t$currentDir\t$ai_useDate\t$fileNameForListFilePlusOneH\t$fileNameForListFileMinusOneH\t$fileNameForListFile\n";
#		close OUTFTPDEFER;

		if(!$ai_upload) {
			AI_logMessage ("$filename: deferring safetyperiod check and download", 302063, $ai_prid,0);
		} else {
			AI_logMessage ("$filename: deferring safetyperiod check and upload", 302064, $ai_prid,0);
		}
		return;
    }

#
    # OB-2675 Need a heartbeat during the ftp phase now, otherwise we get killed off when taking
    # too long to run.
    AI_heartBeat($ai_prid, $ai_defaultProcDirectory) if ($ai_useMonitorFile);

    if ($ai_FTPSafetyPeriod) {
		# safetyperiod > 0, check that we first saw this file at least $ai_FTPSafetyPeriod mins ago
		$scFileName = $fullfilename;
		if (!safetyCheck($scFileName, $filesize)) {
			AI_logMessage ("$filename: In Safety Period ($ai_FTPSafetyPeriod mins) - do not download", 302065, $ai_prid,1);
			return;
		}
    }

    if(!$ai_upload) {
		print "need to download file\n" if ($ai_verbose);
	} else {
		print "need to upload file\n" if ($ai_verbose);
	}
	dbgprint "currentDir: $currentDir\n";
	
	my $currentDirFile = File::Spec->catfile($currentDir, $fullfilename);
	
    ftp_cd($currentDir);

    $file_count++; # move here by naeem 30-june-2006
    my $bFileFound;
    my $targetfile;
    my $outputDirectory;
    my $dirfailoverCount=0;

## TODO (for altFS)
    if($ai_MAXFilesInDownloadDir != 0) {
        while ($ai_localDIRCount[($file_count+$dir_offset+$dirfailoverCount)%$ai_noOutputDirs] >= $ai_MAXFilesInDownloadDir && $dirfailoverCount < $ai_noOutputDirs) {
            $dirfailoverCount++;
        }
        if ($dirfailoverCount >= $ai_noOutputDirs ) {
            AI_logMessage ("All Download Directories have already no of files equal to $ai_MAXFilesInDownloadDir", 302066, $ai_prid,5);
            AI_ftpExit();
            dieNicely("All Download Directories have already no of files equal to $ai_MAXFilesInDownloadDir");
        }
    }
	
	if ($ai_verbose && $currentDir ne $ai_currentRemoteDir)    {
        AI_logMessage ("*** dir mismatch $currentDir vs $ai_currentRemoteDir", 302067, $ai_prid,5);
        dbgprint " dir mismatch $currentDir vs $ai_currentRemoteDir\n";
    }

	my $tempfiledir = $ai_localTempDIR;
    my $tempfileleaf = $fullfilename;
    my $tempfileleaf_orig = $tempfileleaf;

	if ($ai_PrependCDT == 1) {
		if(!$ai_upload) {
			my $collectdatetime;
			set_datetimestamp(\$collectdatetime);
			$tempfileleaf = "CDT_".$collectdatetime."_".$tempfileleaf; # v6.2.0.36
			AI_logMessage ("Collection Date Time (yyyymmddhhmiss $collectdatetime) added to the file name: $tempfileleaf", 302068, $ai_prid,0);
		} else {
			my $collectdatetime;
			set_datetimestamp(\$collectdatetime);
			$tempfileleaf = "CDT_".$collectdatetime."_".$tempfileleaf; # v6.2.0.36
			AI_logMessage ("Upload: Collection Date Time (yyyymmddhhmiss $collectdatetime) added to the file name: $tempfileleaf", 302069, $ai_prid,0);
			
			$tempfilepath = File::Spec->catfile($tempfiledir, $tempfileleaf);
			#my $finalTempName = File::Spec->catfile($currentDir, $filename);
			my $finalTempName = File::Spec->catfile($currentDir, $fullfilename);
			copy("$finalTempName", "$tempfilepath");
		}
	}

    my $tempfilepath = File::Spec->catfile($tempfiledir, $tempfileleaf);
	
	dbgprint "BEFORE remote host --> tempfilepath: $tempfilepath  ------- tempfileleaf: $tempfileleaf\n";

    # if prepending remote host, need to do it on original (downloaded) file by renaming, in order to insert prepended into tar
    # - and update the filename variable ($tempfilepath)
    
	if ($ai_remoteHostReplaced) {
		if (!$ai_upload) {
			my $finaltempleaf;
			$finaltempleaf = $ai_remoteHostReplaced . $tempfileleaf;
			my $finaltemppath = File::Spec->catfile($tempfiledir, $finaltempleaf);
			AI_logMessage ("Prepending remote host: renaming temp file $tempfilepath to $finaltemppath", 302070, $ai_prid, 0);

			move("$tempfilepath", "$finaltemppath");
			$tempfileleaf = $finaltempleaf;
			$tempfilepath = $finaltemppath;
		} else {
			if ($ai_PrependCDT == 1) {
				unlink $tempfilepath;
			}
		
			$tempfileleaf = $ai_remoteHostReplaced . $tempfileleaf;
			$tempfilepath = File::Spec->catfile($tempfiledir, $tempfileleaf);
			my $finalTempName = File::Spec->catfile($currentDir, $filename);
			#my $finalTempName = File::Spec->catfile($currentDir, $fullfilename);
			
			copy("$finalTempName", "$tempfilepath");
			dbgprint "finalTempName: $finalTempName     --xxxx--     tempfilepath: $tempfilepath\n";
		}
	}

	dbgprint "AFTER ai_remoteHostReplaced tempfilepath: $tempfilepath  ------- tempfileleaf: $tempfileleaf\n";
	
    my $remotefile;
    if ($ai_localFile) { # Local
        $remotefile = File::Spec->catfile($currentDir, $filename);
    } elsif ($ai_SFTP==1 or $ai_SFTP==2 or $ai_SFTP==3) { # SFTP
		if(!$ai_upload) {
			$remotefile = "$currentDir"."/$filename";
		} else {
			if($ai_remoteHostReplaced || $ai_PrependCDT) {
				$remotefile = File::Spec->catfile($uploadDir, $tempfileleaf);
				if ($uploadDir =~ /\//o) { # UNIX
					$remotefile =~ s/\\/\//go; # make sure UNIX format
				} elsif ($uploadDir =~ /\\/o) { # Win
					$remotefile =~ s/\//\\/go;
				}
				dbgprint "...remotefile: $remotefile\n";
				my $finalTempName = File::Spec->catfile($currentDir, $filename);
				copy("$finalTempName", "$tempfilepath");
			} else {
				$remotefile = File::Spec->catfile($uploadDir, $fullfilename);
				dbgprint "...remotefile 2: $remotefile\n";
				my $finalTempName = File::Spec->catfile($currentDir, $filename);
				copy("$finalTempName", "$tempfilepath");
			}
		}
    } else { # FTP
		dbgprint "uploadDir: $uploadDir, filename: $filename\n";
	
        if(!$ai_upload) {
			$remotefile = $filename;
		} else {
			if($ai_remoteHostReplaced || $ai_PrependCDT) {
				$remotefile = File::Spec->catfile($uploadDir, $tempfileleaf);
				if ($uploadDir =~ /\//o) { # UNIX
					$remotefile =~ s/\\/\//go; # make sure UNIX format
				} elsif ($uploadDir =~ /\\/o) { # Win
					$remotefile =~ s/\//\\/go;
				}
				dbgprint "...remotefile 3: $remotefile\n";
				my $finalTempName = File::Spec->catfile($currentDir, $filename);
				copy("$finalTempName", "$tempfilepath");
			} else {
				#$remotefile = File::Spec->catfile($uploadDir, $filename);
				$remotefile = File::Spec->catfile($uploadDir, $fullfilename);
				dbgprint "...remotefile 4: $remotefile\n";
				#$tempfilepath = File::Spec->catfile($currentDir, $filename);
				#$tempfilepath = File::Spec->catfile($currentDir, $fullfilename);
				my $finalTempName = File::Spec->catfile($currentDir, $filename);
				copy("$finalTempName", "$tempfilepath");
			}
		}
    }

    if(!$ai_upload) {
		dbgprint "ftp_file download: $remotefile to $tempfilepath\n";
    } else {
		dbgprint "ftp_file upload: $tempfilepath to $remotefile\n";
	}
	my $retry_count=1;
    if ($ai_localFile) { # Local
		if(!$ai_upload) {
			$get_result = copy ($remotefile, $tempfilepath); # 1 is success, 0 failure
		} else {
			$get_result = copy ($tempfilepath, $remotefile); # 1 is success, 0 failure
		}
    } elsif ($ai_SFTP==3) { # external SFTP
		if(!$ai_upload) {
			dbgprint "*** external sftp GET ***\n";
			if ( extSFTP_sendCommand("get $remotefile $tempfilepath") )
			{
				$get_result = 1;
			} else {
				AI_logMessage ("$ai_remoteHost: SFTP get command failed - file $remotefile => $tempfilepath", 302071, $ai_prid, 2);
				$get_result = 0;
			}
		} else {
			dbgprint "*** external sftp PUT ***\n";
			if ( extSFTP_sendCommand("put $tempfilepath $remotefile") )
			{
				$get_result = 1;
				if (!$ai_backup) { 
					unlink $tempfilepath; # remove file from temp folder download
				}
			} else {
				AI_logMessage ("$ai_remoteHost: SFTP put command failed - file $tempfilepath => $remotefile", 302072, $ai_prid, 2);
				$get_result = 0;
			}
		}
    } elsif ($ai_SFTP==1 or $ai_SFTP==2) { # SFTP
		if(!$ai_upload) {
			$ai_ftpHandle->get($remotefile, $tempfilepath);
			$get_result = 1;
		} else {
			for (; $retry_count <= 1; $retry_count++) {
				dbgprint ("SFTP PUT  tempfilepath: $tempfilepath  -+-  remotefile: $remotefile\n");
				$remotefile =~ s/\\/\//go; # to make sure about destination path

				if(!$ai_ftpHandle->put($tempfilepath, $remotefile)) {
					$get_result = 0;
					AI_logMessage ("$ai_remoteHost: FTP Put command failed - file $tempfilepath => $remotefile", 302073, $ai_prid,2);
					AI_logMessage ("Trying reconnection to FTP server $retry_count time", 302074, $ai_prid,1);
					AI_ftpExit();
					AI_connectToHost();
				} else {
					if (!$ai_backup) { 
						unlink $tempfilepath; # remove file from temp folder download
					}
				}
			}
		}
    } else { # FTP
        for (; $retry_count <= 1; $retry_count++) {
            #    dbgprint " going to do ftp get($remotefile, $tempfilepath)\n";
			if($ai_upload) {
				dbgprint ("FTP PUT  tempfilepath: $tempfilepath  -+-  remotefile: $remotefile\n");
				$remotefile =~ s/\\/\//go; # to make sure about destination path

				if(!$ai_ftpHandle->put($tempfilepath, $remotefile)) {
					$get_result = 0;
					AI_logMessage ("$ai_remoteHost: FTP Put command failed - file $tempfilepath => $remotefile", 302075, $ai_prid,2);
					AI_logMessage ("Trying reconnection to FTP server $retry_count time", 302076, $ai_prid,1);
					AI_ftpExit();
					AI_connectToHost();
				} else {
					#if($ai_remoteHostReplaced || $ai_PrependCDT) {
						if (!$ai_backup) { 
							unlink $tempfilepath; # remove file from temp folder download
						}
					#}
				}
			} else {
				if (!$ai_ftpHandle->get($remotefile, $tempfilepath)) {
					$get_result = 0;
					AI_logMessage ("$ai_remoteHost: FTP Get command failed - file $remotefile => $tempfilepath", 302077, $ai_prid,2);
					AI_logMessage ("Trying reconnection to FTP server $retry_count time", 302078, $ai_prid,1);
					AI_ftpExit();
					AI_connectToHost();
					ftp_cd($ai_currentRemoteDir);
				} else {
					$get_result = 1;
					last; #######break;
				}
			}
        }
    }
    if(!$get_result && !$ai_upload) {
        if ($retry_count == 2) {
            AI_logMessage ("$ai_remoteHost: FTP Get command failed - file $remotefile => $tempfilepath 1 times", 302079, $ai_prid,4);
            AI_logMessage ("file $remotefile => $tempfilepath is going to be ignored", 302080, $ai_prid,2);
            return;
        } else {
            AI_logMessage ("$ai_remoteHost: FTP Get command failed - file $remotefile => $tempfilepath", 302081, $ai_prid,4);
            return;
        }
    }
	
	dbgprint "*** filesize: $filesize\n";
    my $tempFileSize = $filesize/1024; # v3.17
    #AI_logMessage ("$ai_remoteHost: FTP File - $remotefile downloaded kB: $tempFileSize", 9, $ai_prid,1);
    if (!$ai_CheckingFileSize) {
        $tempFileSize="<unknown size>";
    }
    if(!$ai_upload) {
		AI_logMessage (join(',',@ai_remoteHosts).": FTP Full File Name: $fullfilename - File: $remotefile downloaded kB: $tempFileSize", 302082, $ai_prid,0);
	} else {
		AI_logMessage (join(',',@ai_remoteHosts).": FTP Full File Name: $fullfilename - File: $remotefile uploaded kB: $tempFileSize", 302083, $ai_prid,0);
	}

    #"FTP login failed for all Hosts (".join(',',@ai_remoteHosts).")"

    # not currently used #  $ai_haveFTPedFiles = 1;
    $totalkbytes += $filesize/1024;

    #first backup the file if required
    if ($ai_backup) {
        ## my $backupfile= "$ai_backupDirectory"."/$fullfilename"; ## original, replaced with below
        my $backupfile;

        $currentBackupFolderName = $ai_backupDirectory;
        
        if ($ai_archiveBackup) {
			my $backupDFLRef = \%backupDFL;
			archive_file(\@backupArchiveInfo, $currentBackupFolderName, $backupDFLRef, $tempfileleaf_orig, $tempfilepath, $filesize);
        } else { # if ($ai_archiveBackup)...
        	if( $ai_UseFolderFileLimit > 0 )  { # if DFL functionality is required
				DFL_GetDir(\%backupDFL);
				$currentBackupFolderName = $backupDFL{'FOLDER'};
			}

#          $backupfile = File::Spec->catfile($currentBackupFolderName, $ai_remoteHostReplaced . $fullfilename);
			$backupfile = File::Spec->catfile($currentBackupFolderName, $tempfileleaf);

			## check if the file already exists
			$bFileFound = 0;
			if ( -f $backupfile ) {
				$bFileFound = 1;
			}

			if (copy("$tempfilepath","$backupfile")) {
				AI_logMessage (join(',',@ai_remoteHosts).": Backed up $tempfilepath to $backupfile", 302084, $ai_prid,0); #3.17

				if( $bFileFound == 0 ) {
					## do not increase the counter if the file is already there
					$backupDFL{'COUNT'}++ if ($ai_UseFolderFileLimit > 0);
				}
				
				if($ai_upload) {
					unlink $tempfilepath;
				}
			} else {
				AI_logMessage ("$ai_remoteHost: File backup [$tempfilepath to $backupfile] failed: $!", 302085, $ai_prid,2);
			}
        } # if ($ai_archiveBackup)...
    }

###
### Done Backup, now do Output
###

    #print " $file_count, $dir_offset , $dirfailoverCount \n" if ($ai_verbose);
    $debugAltFS = 1 if ( ($filename =~ /DEBUG_ENTER_ALT/) );
    $debugAltFS = -1 if ( ($filename =~ /DEBUG_EXIT_ALT/) );

    if ($ai_altFS)
    {
		# in case of multiple output directories, we're only testing the first one
		if (!$ai_altFS_diverting)
		{ # not currently diverting
			if ($debugAltFS == 1 || ExceededOutputUsage())
			{
			  AI_logMessage ("*** Exceeded disk usage on output directory, diverting to alternative filesystem ***", 302086, $ai_prid,4);
			  $ai_altFS_diverting = 1;
			}
		}

		if ($ai_altFS_diverting)
		{ # currently diverting
			if (ExceededAlternativeUsage())
			{
				dieNicely("*** Exceeded disk usage on alternative directory, exiting! ***");
			}
			if ($debugAltFS == -1 || ($debugAltFS == 0 && NormalOutputUsage()))
			{
				AI_logMessage ("*** Disk usage on output directory returned to normal level, reverting to original output filesystem ***", 302087, $ai_prid,2);
				RemoveArchiveLocks(); # remove locks on tar files in alternative directory
				$ai_altFS_diverting = 0;
			}
		}
    }

    if ($ai_altFS_diverting) {
		$outputDirIndex=($file_count+$dir_offset+$dirfailoverCount)%$ai_noAltDirs;
		$outputDirectory = "$ai_altDIR[$outputDirIndex]/";
    } else {
		$outputDirIndex=($file_count+$dir_offset+$dirfailoverCount)%$ai_noOutputDirs;
		$outputDirectory = "$ai_localDIR[$outputDirIndex]/";
		if($ai_MAXFilesInDownloadDir != 0) {
			## TODO altFS ??
			$ai_localDIRCount[($file_count+$dir_offset+$dirfailoverCount)%$ai_noOutputDirs]++;
		}
    }

# going to ignore this option for now...(02/2010, altFS etc)
    if ($ai_tosubdir) {
        my @spliter=split(/,/,$ai_tosubdir);
        my ($basefilename,$t)= split(".csv",$fullfilename);
        my @splitname=split(/$ai_fileDelim/, $basefilename);

        foreach $split (@spliter) {
            $outputDirectory .= $splitname[$split-1]."/";
            if (not -d $outputDirectory) {
                mkdir $outputDirectory;
            }
        }
    }

    my $outputDFLRef;

    ## New Directory File Limit functionality
    ## Keep track of the number of files. Check if <= number of Directory File Limit. Create new subfolder using date time and ID
    if( $ai_UseFolderFileLimit > 0 ) {
#      dbgprint "output dir index = $outputDirIndex, output root dir = $ai_localDIR[$outputDirIndex]\n" if (!$ai_altFS_diverting);
#      dbgprint "alt output dir index = $outputDirIndex, alt output root dir = $ai_altDIR[$outputDirIndex]\n" if ($ai_altFS_diverting);

		if ($ai_altFS_diverting) {
			$outputDFLRef = \%{$altDFLs[$outputDirIndex]};
		} else {
			$outputDFLRef = \%{$outputDFLs[$outputDirIndex]};
		}
    }

    if ($ai_archiveAlt && $ai_altFS_diverting && !$ai_altExtractBeforeArchive)
    {
        archive_file(\@altArchiveInfo, $outputDirectory, $outputDFLRef, $tempfileleaf_orig, $tempfilepath, $filesize);

        # Have appended original file to tar file, now remove original file
		unlink $tempfilepath;
	} else { 
		# NOT archiving (tar) directly into alt output
		if ( $ai_UseFolderFileLimit > 0 && !($ai_altFS_diverting && $ai_archiveAlt) ) # not (divert to alt && archive)
		##      if ( $ai_UseFolderFileLimit > 0 ) # not (divert to alt && archive)
		{
			## DFL, with or without multiple output directories, to normal or alternative directory, but no archive (tar)

			DFL_GetDir($outputDFLRef);
			$outputDirectory = $outputDFLRef->{'FOLDER'};
		}

		$targetfile = File::Spec->catfile($outputDirectory, $tempfileleaf);

		##--
		# Unzip file if required
		if ($ai_unzipCommand && $tempfilepath =~ /$ai_zipExtension/) {
			dbgprint " $ai_unzipCommand \"$tempfilepath\"\n";
			print ("$ai_unzipCommand \"$tempfilepath\"\n") if ($ai_verbose);
			if ((system ("$ai_unzipCommand \"$tempfilepath\" ") ) > 0) {
				AI_logMessage ("$ai_remoteHost: File uncompress failed: $!", 302088, $ai_prid,2);
				#unlink $tempfilepath;
			} else {
				AI_logMessage ("$ai_remoteHost: Uncompressed $tempfilepath", 302089, $ai_prid,1);
			}
			$tempfilepath =~ s/$ai_zipExtension$//;
			$targetfile =~ s/$ai_zipExtension$//;
		}

      ## 3.4-1.. If Untar set - then untar files into directory of targetfile.

	#      if ($ai_untarCommand and $tempfilepath =~ /$ai_tarExtension$/) {
	#          processFilesFromArchiveToDest($tempfilepath, dirname $targetfile, $filesize);
	#   my ($inFile, $targDir, $filesize) = @_;

		my $inFile = $tempfilepath;
		my $targDir = dirname $targetfile;

		my $inDir = dirname $inFile;

		# will use in place dir (temp dir) whether unzipafterTar or not
		chdir $inDir;

		my $didUnTar = 0;

		if ($ai_untarCommand and $tempfilepath =~ /$ai_tarExtension$/) {
			dbgprint " $ai_untarCommand \"$inFile\"\n";
			my $res = system ("$ai_untarCommand \"$inFile\" ");

			if ($res > 0) {
				AI_logMessage ("$ai_remoteHost: File untar failed: $!", 302090, $ai_prid,2);
				#####	  return;
			} else {
				AI_logMessage ("$ai_remoteHost: Untarred $inFile to $inDir", 302091, $ai_prid,1);
				$didUnTar = 1;
				# remove the archive (tar) file
				unlink $inFile;	  
			}
		}

		my $d = new DirHandle "$inDir";

		if (defined $d) {
			my ($filename, $targetfile);
			DirLoop:
			while (defined($filename = $d->read))
			{
				#AI_logMessage ("processFilesFromTempToDest($inDir,$targDir): $filename",210, $ai_prid,1);
				AI_logMessage ("processFilesFromTempToDest($inDir,$targDir): $filename", 302093, $ai_prid,0) if(basename($filename) !~ /^\./o); #if different then . or ..
				
				my $inDirFilename = File::Spec->catfile($inDir,$filename);
				if (-f $inDirFilename)
				{
					if ($didUnTar && $ai_unzipAfterTar && $filename =~ /$ai_zipExtension$/)
					{
					  # need to unzip this one
					  # *** need to have chdir for this one
						if ((system ("$ai_unzipCommand \"$filename\" ") ) > 0)
						{
							AI_logMessage ("$ai_remoteHost: File uncompress '$ai_unzipCommand \"$filename\"' failed: $!", 302094, $ai_prid,2);
							unlink $filename;
							next DirLoop;
						}
						AI_logMessage ("$ai_remoteHost: Uncompressed $filename", 302095, $ai_prid,1);
						$filename =~ s/$ai_zipExtension$//;
					}

					if ( $ai_UseFolderFileLimit > 0 && !($ai_altFS_diverting && $ai_archiveAlt)
						 && $outputDFLRef->{'COUNT'} >= $ai_currentFolderFileLimit 
					   ) {
					  DFL_GetDir($outputDFLRef);
					  $targDir = $outputDFLRef->{'FOLDER'}; 
					}

					## Now remote host is replaced by renaming temp file, so no need here
					## $targetfile = File::Spec->catfile($targDir, $ai_remoteHostReplaced . $filename);
					$targetfile = File::Spec->catfile($targDir, $filename);

					if ($ai_altFS_diverting && $ai_archiveAlt)
					{
						# assert( $ai_altExtractBeforeArchive )
						archive_file(\@altArchiveInfo, $outputDirectory, $outputDFLRef, $tempfileleaf_orig, $inDirFilename, (stat($inDirFilename))[7]);

						# Have appended original file to tar file, now remove original file
						unlink $inDirFilename;
					} else {
						## check if the file already exists

						$bFileFound = 0;
						if ( -f $targetfile ) {
						  $bFileFound = 1;
						}

						# then move from temp area
						if (!$ai_upload && !moveFile($inDirFilename, $targetfile)) {
							AI_logMessage ("$ai_remoteHost: $filename could not be moved to $targetfile: $!\n", 302096, $ai_prid,2);
							## *** BEWARE return from here (currently from sub ftp_file)
							###		  return;
							next DirLoop;
						}

						if ( $bFileFound == 0 ) {
							## do not increase the counter if the file is already there
							$outputDFLRef->{'COUNT'}++ if ( $ai_UseFolderFileLimit > 0  && defined $outputDFLRef) ;
						}
						##print ("DFL folder file count: $outputDFLRef->{'COUNT'}\n") if ($ai_verbose && $ai_UseFolderFileLimit > 0 );

						print ("moved $filename to $targetfile\n") if ($ai_verbose);

						chmod 0666, "$targetfile" ||  AI_logMessage ("$ai_remoteHost: Couldn't change permissions on $targetfile: $!\n", 302097, $ai_prid,2); ## added for air00100716
						if(!$ai_upload) {
							my $size = (stat($targetfile))[7];
							#AI_logMessage("$filename: Filesize server-indicated: $filesize, downloaded: $size", 51, $ai_prid, 1); # original...
							# to have only a final/summary message
							AI_logMessage("Server: ".join(',',@ai_remoteHosts)." - Input file: $remotefile - downloaded kB: $tempFileSize - FTP Full File Name: $fullfilename - Output file: $filename - Filesize server indicated: $filesize - downloaded: $size", 302098, $ai_prid, 1);
						} else {
							my $size = (stat($tempfilepath))[7];
							#AI_logMessage("$filename: Filesize server-indicated: $filesize, uploaded: $size", 51, $ai_prid, 1); # original...
							# to have only a final/summary message
							AI_logMessage("Server: ".join(',',@ai_remoteHosts)." - Input file: $remotefile - uploaded kB: $tempFileSize - FTP Full File Name: $fullfilename - Output file: $filename - Filesize server indicated: $filesize - downloaded: $size", 302099, $ai_prid, 1);
						}
					} # if (...alt archive ) ... else ...
				} # -f <file>
			} # DirLoop while
		} # defined $d
    } # else # NOT archiving (tar) directly into /alt output

    ##
    ## Have done move/unzip etc of tempfile to output/archive
    ##
    
    ##
    ## Tidy up remote end (remote archive and/or remove)
    ##

    ## 3.6-2 done before removeOnDownload - just in case both are set (removeOnDownload is ignored is remoteArchDir is set)
    if($ai_upload && $ai_removeOnDownload) {
		AI_logMessage ("Deleting Local file on upload: $currentDirFile", 302100, $ai_prid,2);
		#dbgprint "DELETE file - filename: $filename, filesize: $filesize, fullfilename: $fullfilename, currentDir: $currentDir, ai_useDate: $ai_useDate, uploadDir: $uploadDir, defer:$defer";
		#dbgprint "DELETE file 2 - currentDirFile: $currentDirFile, tempfilepath: $tempfilepath";
		
# do we need these two?
		unlink $currentDirFile;
		unlink File::Spec->catfile($currentDir, $filename); # to get the original file name before any string replacement

		unlink $tempfilepath;
#		unlink $inDirFilename;

    ## 3.6-2 done before removeOnDownload - just in case both are set (removeOnDownload is ignored if remoteArchDir is set)
    }    elsif ($ai_remoteArchDir ne "") {
        ## Chdir to remoteArchDir to see if it is available.
        if (!ftp_cd($ai_remoteArchDir)) {
            AI_logMessage ("RemoteArchive: Cannot Access Remote directory for archiving file: $ai_remoteArchDir", 302101, $ai_prid,4);
        }
        if (!ftp_cd($currentDir)) {
            AI_logMessage ("RemoteArchive: Cannot re-access the working directory: $currentDir", 302102, $ai_prid,5);
            AI_endProgram($ai_prid, $ai_defaultProcDirectory) if ($ai_useMonitorFile);
            dieNicely("Lost access to $currentDir - Lost position in FTP - exiting!\n");
        }
        my $archResult = 1;
        if ($ai_localFile) { # Local
			$archResult = move($remotefile, File::Spec->catfile($ai_remoteArchDir,$fullfilename));
        } elsif ($ai_SFTP==3) { # external sftp
#          dbgprint "*** external SFTP rename $filename, $ai_remoteArchDir/$fullfilename ***\n";
			$archResult = extSFTP_sendCommand("rename $filename $ai_remoteArchDir/$fullfilename");
        } elsif ($ai_SFTP==1 or $ai_SFTP==2) { # SFTP
     		if(!$ai_upload) {
				$archResult = $ai_ftpHandle->do_rename($filename, $ai_remoteArchDir/$fullfilename);
			}
        } else { # FTP
            $remoteFilename = combineDirFilename($ai_remoteArchDir,$fullfilename);
            $archResult = $ai_ftpHandle->rename($filename, $remoteFilename);
        }
        if (!$archResult) {
            AI_logMessage ("RemoteArchive: Error moving Remote file: $filename to $remoteFilename", 302103, $ai_prid,4);
        } else {
            AI_logMessage ("RemoteArchive: Moved Remote file: $filename to $remoteFilename", 302104, $ai_prid,2);
        }
    }
    ## 3.3-2
    elsif ($ai_removeOnDownload) {
		AI_logMessage ("Deleting Remote file: $filename", 302105, $ai_prid,2);
		if ($ai_localFile) { # Local
			unlink $remotefile;
        } elsif ($ai_SFTP==3) { # external SFTP
			#dbgprint "*** external sftp unlink $remotefile***";
			if (!extSFTP_sendCommand("rm $remotefile"))
			{
				AI_logMessage("external SFTP failed to remove remote file $remotefile", 302106, $ai_prid, 2);
			}
		} elsif ($ai_SFTP==1 or $ai_SFTP==2) { # SFTP
          if(!$ai_upload) {
              $ai_ftpHandle->do_remove($remotefile);
            }
        } else { # FTP
			dbgprint "Removing file: $remotefile";
            $ai_ftpHandle->delete($remotefile);
        }
    }

    ##
    ## Update list file
    ##

 ##--
    ## 3.2-1
    if ($ai_dateFormat eq "0") {
        $ai_lastListFile = "lastfiles.log";

        if($ai_WindowsDSTFix == 1) {
			$ai_lastListFilePlusOneHour = "lastfilesPlusOneH.log";
			$ai_lastListFileMinusOneHour = "lastfilesMinusOneH.log";
		}
    } elsif (exists $lastFileList{$ai_useDate}) {
        $ai_lastListFile = "lastfiles.$lastFileList{$ai_useDate}";

        if($ai_WindowsDSTFix == 1) {
			$ai_lastListFilePlusOneHour = "lastfilesPlusOneH.$lastFileList{$ai_useDate}";
			$ai_lastListFileMinusOneHour = "lastfilesMinusOneH.$lastFileList{$ai_useDate}";
		}
    } else {
        $ai_lastListFile = "lastfiles.unknown.log";
        AI_logMessage ("ftp_file: $ai_useDate is not known. Unexpected error has occured\n", 302107, $ai_prid,3);
    }

    $fullLastListFile = "$ai_listDirectory"."/$ai_lastListFile";
    my $outfile;
    my $outfileplusone;
    my $outfileminusone;
#    if(!(open (OUTFILE, ">>$fullLastListFile"))) {
    if (!(fetchHandle(\$outfile, "list", ">>$fullLastListFile"))) {
        AI_logMessage ("Couldn\'t open file $fullLastListFile", 302108, $ai_prid,5);
        AI_endProgram($ai_prid, $ai_defaultProcDirectory) if ($ai_useMonitorFile);
        dieNicely("Couldn\'t open file $fullLastListFile\n");
    }

    if($ai_WindowsDSTFix == 1) {
	    $fullLastListFilePlusOneH = "$ai_listDirectory"."/$ai_lastListFilePlusOneHour";
	    $fullLastListFileMinusOneH = "$ai_listDirectory"."/$ai_lastListFileMinusOneHour";
    
#	    if(!(open (OUTFILEPLUSONE, ">>$fullLastListFilePlusOneH"))) {
	    if(!(fetchHandle(\$outfileplusone, "list", ">>$fullLastListFilePlusOneH"))) {
			AI_logMessage ("Couldn\'t open file $fullLastListFilePlusOneH", 302109, $ai_prid,5);
			AI_endProgram($ai_prid, $ai_defaultProcDirectory) if ($ai_useMonitorFile);
			dieNicely("Couldn\'t open file $fullLastListFilePlusOneH\n");
		}
	
#	    if(!(open (OUTFILEMINUSONE, ">>$fullLastListFileMinusOneH"))) {
	    if(!(fetchHandle(\$outfileminusone, "list", ">>$fullLastListFileMinusOneH"))) {
			AI_logMessage ("Couldn\'t open file $fullLastListFileMinusOneH", 302110, $ai_prid,5);
			AI_endProgram($ai_prid, $ai_defaultProcDirectory) if ($ai_useMonitorFile);
			dieNicely("Couldn\'t open file $fullLastListFileMinusOneH\n");
		}
     }
	
# old comment before introduction of $fileNameForListFile instead of $fullfilename
#   if we want the list file to have the files with remote host replaced, use $tempfileleaf here instead of $fullfilename...

    $fileNameForListFile =~ s/$ai_zipExtension$// if ($ai_removeZipExt); ## 3.6-1
	
    if($ai_WindowsDSTFix == 1) {
		$fileNameForListFilePlusOneH =~ s/$ai_zipExtension$// if ($ai_removeZipExt); ## 3.6-1
		$fileNameForListFileMinusOneH =~ s/$ai_zipExtension$// if ($ai_removeZipExt); ## 3.6-1
	    $fileNameForListFile = "${currentDir}|$fileNameForListFile";
	    $fileNameForListFilePlusOneH = "${currentDir}|$fileNameForListFilePlusOneH";
	    $fileNameForListFileMinusOneH = "${currentDir}|$fileNameForListFileMinusOneH";
    }

    print $outfile "$fileNameForListFile\n";
#    print OUTFILE ("$fileNameForListFile\n");
#    close OUTFILE;
	
    if ($ai_WindowsDSTFix == 1) {
	    print $outfileplusone "$fileNameForListFilePlusOneH\n";
#	    print OUTFILEPLUSONE ("$fileNameForListFilePlusOneH\n");
#	    close OUTFILEPLUSONE;
	    print $outfileminusone "$fileNameForListFileMinusOneH\n";
#	    print OUTFILEMINUSONE ("$fileNameForListFileMinusOneH\n");
#	    close OUTFILEMINUSONE;
    }
	
    if ($ai_dateFormat eq "0") {
		$lastFileList{$fileNameForListFile}=1;
    } else {
		$lastFileList{$ai_useDate}{$fileNameForListFile}=1;
    }
	    
    if($ai_WindowsDSTFix == 1) {
	    if ($ai_dateFormat eq "0") {
		$lastFileListPlusOneH{$fileNameForListFilePlusOneH}=1;
		$lastFileListMinusOneH{$fileNameForListFileMinusOneH}=1;
	    } else {
		$lastFileListPlusOneH{$ai_useDate}{$fileNameForListFilePlusOneH}=1;
		$lastFileListMinusOneH{$ai_useDate}{$fileNameForListFileMinusOneH}=1;			
	    }
    }


    ##
    ## Update safetycheck records for the file
    ##

    # make sure the safetycheck records for this file are wiped
    delete $fileTimes{$scFileName} if ($ai_FTPSafetyPeriod);
    delete $fileSizes{$scFileName} if ($ai_FTPSafetyPeriod);

    $ai_noFiles++;
    #--
} # ftp_file


sub ftp_cd {
	my $dirName = shift(@_);

	dbgprint "ftp_cd $$ handle=$ai_ftpHandle [$ai_currentConnection] : $dirName\n";

	$ai_currentRemoteDir = $dirName;
	if(!$ai_upload) {
		if ($ai_SFTP == 3)
		{
			print "ftp_cd (sftp 3): $dirName\n" if ($ai_verbose);
			if ( !extSFTP_sendCommand("cd $dirName"))
			{
				AI_logMessage("$ai_remoteHost: SFTP CWD to $dirName command failed", 302111, $ai_prid,2);
				return 0;
			}
		} elsif (!$ai_SFTP>=1 && !$ai_localFile)
		{
			print "ftp_cd: $dirName\n" if ($ai_verbose);
			if (!$ai_ftpHandle->cwd($dirName))
			{
				AI_logMessage ("$ai_remoteHost: FTP CWD to $dirName command failed", 302113, $ai_prid,2);
				return 0;
			}
		}
	} # if($!$ai_upload)
  return 1;
}

sub dir_date_name {
    #  Arguments:
    #  1. Offset
    my $dayOffset = shift(@_);
    ($Seconds, $Minute, $Hour, $DayOfMonth, $Month, $Year, $WeekDay, $DayOfYear, $IsDST) = localtime($ai_starttime - 24*60*60*$dayOffset);
    my $dateString = sprintf("%4d%02d%02d", $Year + 1900, $Month + 1, $DayOfMonth);
    return "$dateString";
}

sub file_date_name {
    #  Arguments:
    #  1. Offset
    my $dayOffset = shift(@_);
    my ($DD, $MM, $YYYY) = (localtime($ai_starttime - 24*60*60*$dayOffset))[3,4,5];
    my $dateString = $ai_dateFormat;
    $YYYY += 1900;
    my $YY = substr($YYYY,2);
    my $MTH = uc($MONTHS[$MM]);
    my $Mth = $MONTHS[$MM];
    #$MT = uc($MTH[$MM]); # commented out v3.17
    my $Mt2 = $MTH2[$MM]; # v3.17
    my $MT2 = uc($MTH2[$MM]);

    $MM += 1;

    my $MMS = $MM; # added in v6.2.0.47 to cope with date format M-D-YYYY
    my $DDS = $DD; # added in v6.2.0.47 to cope with date format M-D-YYYY

    if ($MM < 10) {
        $MM="0$MM";
    }
    if ($DD < 10) {
        $DD="0$DD";
    }
    
# search/replace in the best order to avoid interference
# Beware of M-replacement clobbering MTH/Mth
# Beware of MTH/Mth-replacement generating new letters to replace (M of May, D of Dec)
# So, D before MTH/Mth and MTH/Mth => not M (only need one of M, MM, MTHn, Mthn)

    $dateString =~ s/YYYY/$YYYY/;
    $dateString =~ s/YY/$YY/;
    $dateString =~ s/MM/$MM/;
    $dateString =~ s/DD/$DD/;
    $dateString =~ s/D/$DDS/; # added in v6.2.0.47 to cope with date format M-D-YYYY

    if ($ai_dateFormat =~ /MTH([0-9]+)/o) {
        $len=$1;
        if ($len==2) {
            $replace=$MT2;
        } # v3.17
        #{ $replace=$MT; } # commented out v3.17
        #elsif ($len==4) # commented out v3.17
        #{ $replace=$MT2; } # commented out v3.17
        else {
            $replace=substr($MTH,0,$len);
        }
        $dateString =~ s/M[tT][hH]?$len/$replace/;
    } elsif ($ai_dateFormat =~ /Mth([0-9]+)/o) # added v3.17
    {
        $len=$1;
        if ($len==2) {
            $replace=$Mt2;
        } else {
            $replace=substr($Mth,0,$len);
        }
        $dateString =~ s/M[tT][hH]?$len/$replace/;
    } else
    {
	$dateString =~ s/M/$MMS/; # added in v6.2.0.47 to cope with date format M-D-YYYY
    }

    $dateString =~ s/[\^\$]//go;

    return "$dateString";
}

sub extSFTP_debug
{
#  print "before: '". $ai_ftpHandle->before(). "'\n";
#  print "match : '". $ai_ftpHandle->match(). "'\n";
#  print "after : '". $ai_ftpHandle->after(). "'\n";
}

sub extSFTP_wait
{
	my $timeout = shift;
	# By default, if $timeout is not defined, pass through "undef" to the call to expect
	# which means wait forever

	###   $timeout = 100 if (!defined $timeout);

	$ai_ftpHandle->expect($timeout, "sftp\> ");
}

sub extSFTP_noProblem
{
	return 0 if (! defined ($ai_ftpHandle->match()));
	my $checkOutput = $ai_ftpHandle->before();
	return 0 if ($checkOutput =~ /Connection.*closed/o);
	return 1;
}

sub extSFTP_sendCommand
{
	# Send a command to the external sftp executable
	my $cmd = shift;
	$ai_ftpHandle->send("$cmd\n");
	extSFTP_wait();
	extSFTP_debug();

	# Some limited error checking
	if (!defined $ai_ftpHandle->match())
	{
		dbgprint "*** extSFTP_sendCommand '$cmd' didn't see sftp prompt ***\n";
		return 0;
	}
	my $checkOutput = $ai_ftpHandle->before();
	if ( $checkOutput =~ /Couldn\'t .*\:/o || $checkOutput =~ /Connection closed/o )
	{
		dbgprint "*** extSFTP_sendCommand '$cmd' detected error ***\n";
		return 0;
	}
  return 1;
}

sub extSFTP_ls
{
	my $arrayRef = shift;
	my $inDir = shift;
	### $ai_ftpHandle->send("$extSFTP_lsCommand $inDir\n");
	### new version - use cd to current dir and ls -l to avoid unnecessary full paths in the ls -l output  
	#  dbgprint "extSFTP_ls $extSFTP_lsCommand\n";
	$ai_ftpHandle->send("$extSFTP_lsCommand\n");
	extSFTP_wait();
	extSFTP_debug();

	@{$arrayRef} = split("\r?\n", $ai_ftpHandle->before());
	#  print "eg ${$arrayRef}[0], ${$arrayRef}[1]\n";
}

sub extSFTP_quit
{
	if (defined $ai_ftpHandle)
	{
		$ai_ftpHandle->hard_close();
		undef $ai_ftpHandle;
	}
}

sub AI_connectToHost {
# now returns the handle for the connection

    AI_logMessage ("AI_connectToHost", 302114, $ai_prid, 0);

	if ($ai_localFile) {
		return 0;
	}

	my ($connected)=0; my ($i)=0; ##SBG  v3.16
	my $packageNOTfound; # undefined so far

        if ($ai_SFTP==3) { # external SFTP
			my $evalRes = eval {
				require Expect;
				import Expect;
				1;
			};
			if (! defined $evalRes) {
				dieNicely("Expect package was NOT found!");
			}
        } elsif ($ai_SFTP==2) {
			$packageNOTfound = eval{
				require Net::SFTP::Foreign::Compat; # v6.2.0.21
				import NET::SFTP::Foreign::Compat; # v6.2.0.21
				$packageNOTfound=1;
			};

			#if($@) ## we could use $@ to check for null value but it did not work as expected
			if (not defined $packageNOTfound) {
				dieNicely("Net::SFTP::Foreign package was NOT found!"); # it logs a critical message too
			}
        } elsif ($ai_SFTP==1) {
            $packageNOTfound = eval {
                require Net::SFTP;
                import NET::SFTP; # 3.14 air00089009 and 3.17 air00085217
                $packageNOTfound=1;
            };
            if (not defined $packageNOTfound) {
                dieNicely("Net::SFTP package was NOT found!");
            }
        }

    my $newPort;

	foreach $rh (@ai_remoteHosts) {
		$ai_remoteHost = $rh;
		$newPort = 0;

		# Test for match of host:port-number + trailing whitespace
		if ($ai_remoteHost =~ /(.*)\:(\d+)\s*$/o)
		{
			my ($host, $port) = ($1, $2);
			if (defined $1 && defined $2) {
				$ai_remoteHost = $host;
				$newPort = $port;
			}
		}

		dbgprint "host=$ai_remoteHost, newport=$newPort\n";

        if ($ai_SFTP==3)
		{ # external sftp
            my $fail = 0;
            dbgprint "*** external sftp set up parameters ***\n";
            $ai_ftpHandle = new Expect();

            $ai_ftpHandle->log_stdout(0); # don't echo output to stdout/console
            $ai_ftpHandle->raw_pty(1);    # disable echo of sent commands, and disable CR->LF translation

			@spawnParams=($extSFTP_executable);
			push @spawnParams, "-oBindAddress=$ai_BindAddress" if ($ai_BindAddress);
			push @spawnParams, "-oPort=$newPort" if ($newPort);
			push @spawnParams, "${ai_username[$i]}\@$ai_remoteHost";
			dbgprint "SFTP=3, username: ${ai_username[$i]}, remoteHost: $ai_remoteHost, BindAddress: $ai_BindAddress, port: $newPort\n";

			if (! $ai_ftpHandle->spawn(@spawnParams)) {
                $fail = 1;
            }
            if (!$fail) {
                extSFTP_wait(30); # shouldn't have to wait more than 30 seconds for a connection
                $fail = 1 if (!extSFTP_noProblem());
				dbgprint "external sftp spawned\n";
				extSFTP_debug();
            }
            if ($fail) {
				extSFTP_quit(); # includes undef $ai_ftpHandle
				AI_logMessage ("Failed external SFTP connection to $ai_remoteHost", 302115, $ai_prid, 2);
            }
		} elsif ($ai_SFTP==2) { ## NEW package usage
            %args1 = ();
            $args1{"user"} = $ai_username[$i];
			$args1{"port"} = $newPort if ($newPort);
#			$args1{"BindAddress"} = $ai_BindAddress if ($ai_BindAddress);
			$args1{"more"} = [-o => "BindAddress = $ai_BindAddress"] if ($ai_BindAddress);
            $args1{"password"} = truePassword($i) if ($using_password);
			# autodisconnect = 2 --> Disconnect on exit from the current process only
			# (otherwise child processes can kill the parent's sftp connection)
			$args1{"autodisconnect"} = 2;
            ##$args1{"password"} = truePassword($i); ## password is not required as it is used SSH public key
            eval { $ai_ftpHandle = Net::SFTP::Foreign::Compat->new($ai_remoteHost,%args1); };
			if ($@)
			{
				AI_logMessage ("Failed SFTP connection to $ai_remoteHost: $@", 302116, $ai_prid, 2);
				undef $ai_ftpHandle;
			}
			if (defined $ai_ftpHandle)
			{
				my ($statusCode, $statusText) = $ai_ftpHandle->status();
				if ($statusCode) 
				{
					AI_logMessage ("Failed SFTP connection to $ai_remoteHost: $statusText", 302117, $ai_prid, 2);
					undef $ai_ftpHandle;
				}
			}
        } elsif ($ai_SFTP==1) { # old SFTP package
            %args1 = ();
            $args1{"user"} = $ai_username[$i];
            $args1{"password"} = truePassword($i);
            $args1{"debug"} = $ai_debug;
			%sshargs = ();
            if( $ai_SSHargs == 1 ) { 
				$sshargs{"compression"} = 1; # OB-733
			}
			$sshargs{"port"} = $newPort if ($newPort);
#			$sshargs{"BindAddress"} = $ai_BindAddress if ($ai_BindAddress);
			$sshargs{"options"} = [ "BindAddress $ai_BindAddress" ] if ($ai_BindAddress);
			$args1{"ssh_args"} = [ %sshargs ];
            eval { $ai_ftpHandle = Net::SFTP->new($ai_remoteHost,%args1); };
			if ($@)
			{
				undef $ai_ftpHandle;
				AI_logMessage ("Failed SFTP connection to $ai_remoteHost: $@", 302118, $ai_prid, 2);
			}
        } else { # FTP
			%args1 = ();
			$args1{"Passive"} = 0 if ($ai_FTPActive);
			$args1{"Port"} = $newPort if ($newPort);

            if ($ai_FTPActive) {
				dbgprint " new FTP handle, ACTIVE\n";
            } else {
                dbgprint " new FTP handle, PASSIVE\n";
            }

			$ai_ftpHandle = Net::FTP->new($ai_remoteHost, %args1);
            #$ai_ftpHandle = Net::FTP->new($ai_remoteHost, Debug => 1, Passive => 0);
        }

        if (!defined($ai_ftpHandle)) {
            AI_logMessage ("Failed to connect to host: $ai_remoteHost", 302119, $ai_prid, 2);
        } else {
            AI_logMessage ("Connected to host: $ai_remoteHost", 302120, $ai_prid, 0);
            $connected=1; ##SBG v3.16
        }

        if(!$ai_SFTP and $connected) {
            if(!($ai_ftpHandle->login($ai_username[$i], truePassword($i)))) {
                AI_logMessage ("$ai_remoteHost: FTP login failed - user: $ai_username[$i] (check password)", 302121, $ai_prid, 2);
                $connected=0; ## SBG v3.16
                $ai_ftpHandle->quit; ##SBG v3.16
                undef $ai_ftpHandle; ##SBG v3.16
            } else {
                print "Logged in as: $ai_username[$i] \n" if ($ai_verbose);
            }
            if ($connected and $ai_FTPType eq "BINARY") {
                $ai_ftpHandle->binary();
            }
        }
        ## SBG v3.16 ###
        if ($connected) {
            last;
        }
        $i++;
        ##-
    }

    if ($ai_PrependHostname)
    {
        $ai_remoteHostReplaced = $ai_remoteHost;
        # replace "." with "_" for PrependHostname option, then add separator to make ready for prepend
        $ai_remoteHostReplaced =~ s/\./_/go;
        $ai_remoteHostReplaced .= $ai_PrependSeparator;
    } else
    {
        $ai_remoteHostReplaced = "";
    }

    ## SBG v3.16 ###
    if (!$connected) {
        AI_logMessage ("FTP login failed for all Hosts (".join(',',@ai_remoteHosts).")", 302122, $ai_prid, 5);
        AI_endProgram($ai_prid, $ai_defaultProcDirectory) if ($ai_useMonitorFile);
        dieNicely("Cannot connect to remoteHosts (".join(',',@ai_remoteHosts).")");
    }
    ##-

    return $ai_ftpHandle;
}

sub AI_ftpExit {
    if (($ai_SFTP==1 or $ai_SFTP==2) || $ai_localFile) {
        return;
    }

    if ($ai_SFTP==3) {
		dbgprint "*** external sftp QUIT ***\n";
		extSFTP_quit();
		return;
    }

    if (!$ai_ftpHandle->quit) {
        AI_logMessage ("$ai_remoteHost: FTP Quit command failed", 302123, $ai_prid, 2);
    }
}

sub AI_logMessage {
    #  Arguments:
    #  1. Message
    #  2. Message ID (optional)
    #  3. PRID (optional)
    #  4. Severity (optional)
    #  5. Log directory (optional)
    #  6. Log filename (optional)
    my $logfile;
    my $ai_defaultID = 0;
    my $ai_defaultPRID = "000000000";
    my $ai_defaultSeverity = 3;
                            #(0, 1, 2, 3,...
    my @ai_severityEnum = ("DEBUG", "INFORMATION", "WARNING", "MINOR", "MAJOR", "CRITICAL");

    my $ai_logMessage = $_[0];
    my $ai_logMessageID = $_[1];
    my $ai_logPrid = $_[2];
    my $ai_logSeverity = $_[3];
    my $ai_logDirectory = $_[4];
    my $ai_logFilename = $_[5];
    #if (!defined($ai_logMessage)) {return };
    if (!defined($ai_logMessage)) {
        return;
    }

    #if (!defined($ai_logSeverity))
    #{  $ai_logSeverity = $ai_defaultSeverity  };
    if (!defined($ai_logSeverity)) {
        $ai_logSeverity = $ai_defaultSeverity;
    }

    #if ($ai_debug > $ai_logSeverity)
    if ( ($ai_Log_Severity-1) > $ai_logSeverity) # v3.17
    {
        return;
    } ##3.4-5

    if (!defined($ai_logMessageID)) {
        $ai_logMessageID = $ai_defaultID;
    }
    if (!defined($ai_logPrid)) {
        $ai_logPrid = $ai_defaultPRID;
    };

    if (!defined($ai_logDirectory)) {
        $ai_logDirectory = $ai_defaultLogDirectory;
    }

    ($Seconds, $Minute, $Hour, $DayOfMonth, $Month, $Year, $WeekDay, $DayOfYear, $IsDST) = localtime(time);
    my $dateString = sprintf("%4d%02d%02d", $Year + 1900, $Month + 1, $DayOfMonth);
    my $filedateString = sprintf("%4d-%02d-%02d", $Year + 1900, $Month + 1, $DayOfMonth);
    my $timeString = sprintf("%02d:%02d:%02d", $Hour, $Minute, $Seconds);
    my $filebase = basename($0, ".pl");
    if (!defined($ai_logFilename)) {
        $ai_fullLogfilename = "$ai_logDirectory"."/$hostName"."_"."$filebase"."_$ai_logPrid";
        $ai_fullLogfilename .= "_$dateString".".log";
    } else {
        $ai_fullLogfilename = "$ai_logDirectory"."/$ai_logFilename";
    }

    my $ai_severityString = $ai_severityEnum[$ai_logSeverity];

## for debugging processes    $ai_severityString .= "_$$";

# print before log to file, in case of problem in opening files
    print("$hostName\t$ai_logPrid\t$filedateString\t$timeString\t$ai_severityString"."\t$ai_logMessageID"."\t$ai_logMessage\n") if ($ai_verbose);

#    open(OUTFILE, ">>$ai_fullLogfilename") || return;
#    print OUTFILE ("$hostName"."\t$ai_logPrid"."\t$filedateString"."\t$timeString"."\t$ai_severityString"."\t$ai_logMessageID"."\t$ai_logMessage\n");
#    close OUTFILE;
    fetchHandle(\$outfile, "log", ">>$ai_fullLogfilename") || return;
    print $outfile ("$hostName"."\t$ai_logPrid"."\t$filedateString"."\t$timeString"."\t$ai_severityString"."\t$ai_logMessageID"."\t$ai_logMessage\n");

    return;
}

sub AI_startProgram {
    #  Arguments:
    #  1. PRID
    #  2. Allow Multiple Instances (optional)
    #  3. Process Directory (optional)
    my $ai_procPrid = $_[0];
    my $ai_multiple = $_[1];
    my $ai_procDirectory = $_[2];
    if (!defined($ai_procDirectory)) {
        $ai_procDirectory = $ai_defaultProcDirectory;
    }

    if (!defined($ai_multiple)) {
        $ai_multiple = 0;
    }

    my $programName = $0;
    my $programID = $$;
    my $filebase = basename($0, ".pl");

    my $pid_name = "$ai_procDirectory"."/$hostName"."_"."$filebase"."_"."$ai_procPrid";
    ##my $pid_name = "$ai_procDirectory"."/$filebase"."_"."$ai_procPrid"; # air00078228
    $pid_name .= ".pid";

    if (-f $pid_name && (!($ai_multiple))) {
        print ("$ai_procPrid: $programName already running\n");
        AI_logMessage ("$ai_procPrid: $programName already running", 302124, $ai_prid, 5);
        exit;
    }

    open(OUTFILE, ">$pid_name") || die "\nUnable to create PID file '$pid_name', check directory and permissions";
    print OUTFILE ("[PID]\n");
    print OUTFILE ("PID=$programID\n");
    close OUTFILE;
    $SIG{INT} = 'sig_handler';

    $log_started = 1;
    return;
}

sub ftouch
{
	$fname = shift;
	$t = time;
	utime $t, $t, $fname;
}

sub AI_heartBeat {
    # Don't want to run this too often - if was run recently, defer
    my $hbtime=time();
    if (defined $lastHeartBeatTime)
    {
		if ($hbtime - $lastHeartBeatTime < $ai_heartBeatDeferTime)
		{
			return;
		}
    }
    $lastHeartBeatTime = $hbtime;

    #  Arguments:
    #  1. PRID
    #  2. Process Directory (optional)
    my $ai_procPrid = $_[0];
    my $ai_procDirectory = $_[1];
    if (!defined($ai_procDirectory)) {
        $ai_procDirectory = $ai_defaultProcDirectory;
    }

    my $programName = $0;
    my $filebase = basename($0, ".pl");

    my $pid_name = "$ai_procDirectory"."/$hostName"."_"."$filebase"."_"."$ai_procPrid";
    $pid_name .= ".pid";

	if( -f $pid_name ) {
		ftouch($pid_name);
	} else {
		$missingPID=1;
		AI_ftpExit();
		dieNicely("PID file  $pid_name  was not found therefore exiting!");
	}
    return;
}

sub AI_endProgram {
    #  Arguments:
    #  1. PRID
    #  2. Process Directory (optional)
    my $ai_procPrid = $_[0];
    my $ai_procDirectory = $_[1];

    AI_logMessage ("End Program.  $opxProgramName $ai_opxFTP_version", 302125, $ai_prid, 1);

    RemoveArchiveLocks();

    if (!defined($ai_procDirectory)) {
		$ai_procDirectory = $ai_defaultProcDirectory;
    }

    my $filebase = basename($0, ".pl");

    my $pid_name = "$ai_procDirectory"."/$hostName"."_"."$filebase"."_"."$ai_procPrid";
    $pid_name .= ".pid";
	if(!$missingPID) {
		unlink $pid_name;
	}
    return;
}

sub combineDirSubdir {
    $dir = shift; # abc:[def.ghi] if vms style
    $subdir = shift; # xyz
    if ($ai_vmsStyle) {
        $newDir = $dir;
        $newDir =~ s/\]$/\.$subdir\]/; # abc:[def.ghi.xyz]
        ##    dbgprint " combineDirSubdir: $dir, $subdir, $newDir\n";
    } else {
        $newDir = "${dir}/$subdir";
    }
    return $newDir;
}

#    if ($ai_vmsStyle) {
#        $newDirectory = "$fileName"; # do relative path for cwd
#    } else {
#        $newDirectory = "$inDir/$fileName";
#    }

sub combineDirFilename {
    $dir = shift; # abc:[def.ghi] if vms style
    $fileName = shift; # xyz
    if ($ai_vmsStyle) {
        # if (! $ai_vmsStyle =~ /\]$/; ) { ??? expect dir to end in [...] }
        $newFileName = "$dir$fileName"; # abc:[def.ghi]xyz
    } else {
        $newFileName = "${dir}/$fileName";
    }
}

sub truePassword {
    $i = shift;
    if (!$using_password) {
	  dieNicely("$ARGV[0]: Unexpected request for password");
	}
	if (!defined($ai_password[$i])) {
        dieNicely("$ARGV[0]: Password no. " . ($i+1) . " not found");
    }
    return $ai_password[$i];
    # v6.2.0.3 now passwords are decrypted all in one string ($temp_password above), not individually
    #    if ($ai_encryptedPasswords != 1)
    #    {
    #    return $ai_password[$i];
    #    }
    #    if (!defined $ai_truePassword[$i])
    #    {
    #    if (!defined(&PLEX_Decode)) { dieNicely("$ARGV[0]: Unable to read password no. ". ($i+1)); }
    #    # This probably means user isn't running the script in its wrapped form
    #    # after using the PLEX tool (with DECRYPT option)
    #    $ai_truePassword[$i] = PLEX_Decode($ai_password[$i]);
    #    }
    #    return $ai_truePassword[$i];
}

sub moveFile {
# WAS "move but prepend remoteHostname if required"
    my $src = shift;
    my $dest = shift;
## NOW remote host is replaced by renaming temp file, so no need here
##    my $finaldest = File::Spec->catfile(dirname($dest), $ai_remoteHostReplaced . basename($dest));
    move "$src", "$dest";
}

sub dieNicely {
    $mess = shift;
    AI_logMessage ( $mess,  302126, $ai_prid, 5);
    AI_endProgram($ai_prid, $ai_defaultProcDirectory) if ($ai_useMonitorFile);
    die "\n".$mess;
}

my %hmap;

sub fetchHandle
{
    my $href = shift; # reference to a scalar - to return the handle
    my $hgroup = shift;
    my $fname = shift; # filename for open, e.g. ">>dir/file-to-append"

    return 0 if (!defined $href || !defined $hgroup || !defined $fname);

    $retrievedHandle = $hmap{$hgroup}{$fname};
    if (defined $retrievedHandle)
    {
	dbgprint "fetchHandle retrieved for $fname\n";
	${$href} = $retrievedHandle;
	return 1;
    }

    local *FH;
    if (!open (FH, "$fname"))
    {
	return 0;
    }

    # minimize chance of multiple processes writing to the same file mangling the output (interleaved lines)
    # This has the effect of flushing after every print (typically every line).
    FH->autoflush(1);

    ${$href} = *FH;

    dbgprint "fetchHandle NEW for $fname\n";
    $hmap{$hgroup}{$fname}=${$href};

    return 1;
}

sub closeHandles
{
    my $hgroup = shift;
    return 0 if (!defined $hgroup);

    for my $v (values %{$hmap{$hgroup}})
    {
	dbgprint "closeHandles\n";
	close $v;
    }
    delete $hmap{$hgroup};
}
