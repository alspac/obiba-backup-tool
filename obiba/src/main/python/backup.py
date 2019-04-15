#!/usr/bin/env python

####################################################################################################################
# 'backup.py' is a simple script used to backup Obiba products on a server.
#
# For each project, files, folders and databases (MySQL and MongoDB) are backed up as specified in the backup.conf. 
# Backups are stored in date structured directory (e.g. destination/project/year/month/day&time/)
# A clean up schedule deletes old backups as specified in the backup.conf
# The current local backup is copied to a remote backup location
# Additional folders may also be copied to the remote  backup location
#
####################################################################################################################

import os
from datetime import datetime
from datetime import date
import subprocess
import gzip
from subprocess import call
import shutil
import traceback
import yaml
import glob
import shlex


class ObibaBackup:
    CONFIG_FILE = os.path.join(os.path.dirname(__file__), "backup.conf")

    def run(self):
        """
        This is where everything starts
        """
        try:
            print "# Obiba backup started (%s)" % datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.__loadConfig()
            self.__setup()
            self.__backupRemoteProjects()
            self.__backupProjects()
            self.__rsyncCleanup()
        except Exception, e:
            print '*' * 80
            print "* ERROR"
            print
            print traceback.format_exc()
            print '*' * 80
        finally:
            print "# Obiba backup completed (%s)" % datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    ####################################################################################################################
    # P R I V A T E     M E T H O D S
    ####################################################################################################################

    def __loadConfig(self):
        configFile = open(self.CONFIG_FILE, 'r')
        self.config = yaml.load(configFile)
        configFile.close()

    ####################################################################################################################
    def __setup(self):
        """
        Setup basically creates the daily backup folder for each project
        """
        #Local backup folder
        backupFolder = self.config['destination']
        self.__createBackupFolder(backupFolder)

        # create the project based backup folder
        today = date.today()

        if 'projects' in self.config:
            for project in self.config['projects'].iterkeys():
                timestamp = datetime.now().strftime('%d-%H-%M-%S')
                backupDestination = os.path.join(backupFolder, project, str(today.year)+'-'+today.strftime('%m'), timestamp)
                self.__createBackupFolder(backupDestination)
                self.config['projects'][project]['destination'] = backupDestination
                
        #Remote backup folder
        if 'rsync' in self.config:
            backupFolder = self.config['rsync']['destination']
            self.__createBackupFolder(backupFolder)
    
            # create the date based backup folder
            today = date.today()
            timestamp = datetime.now().strftime('%d-%H%M%S')
            backupDestination = os.path.join(backupFolder, str(today.year)+'-'+today.strftime('%m'), timestamp)
            self.__createBackupFolder(backupDestination)
            self.config['rsync']['destination'] = backupDestination                

    ####################################################################################################################
    def __backupRemoteProjects(self):
        if 'rsyncs' in self.config:
            for rsync in self.config['rsyncs']:
                if 'folder' in rsync:
                    self.__backupToRemoteServer(rsync['folder'])

    ####################################################################################################################
    def __backupProjects(self):
        if 'projects' in self.config:
            for project in self.config['projects'].iterkeys():
                print "Backing up %s..." % project
                self.__backupProject(self.config['projects'][project], project)

    ####################################################################################################################
    def __backupProject(self, project, projectName):
        destination = project['destination']
        self.__cleanup(os.path.dirname(destination), projectName)
        if 'files' in project:
            self.__backupFiles(project['files'], destination)
        if 'folders' in project:           
            self.__backupFolders(project['folders'], destination)
        if 'mongodbs' in project:
            self.__backupMongodbs(project['mongodbs'], destination)
        if 'databases' in project:
            self.__backupDatabases(project['databases'], destination)

        source = {}
        source['path'] = destination
        self.__backupToRemoteServer(source, projectName)

    ####################################################################################################################
    def __backupToRemoteServer(self, source, remote=None):
        if 'rsync' in self.config: 
            if 'destination' in self.config['rsync']:
                excludes = []
                if 'excludes' in source:
                    for exclude in source['excludes']:
                        excludes.append('--exclude')
                        excludes.append('%s' % exclude)
                        
                publicKey = ''
                remove_source_files = ''

                #Encrypt before copying remotely if required
                if 'encrypt_files' in self.config['rsync']:
                    if 'encryptionPassword' in self.config['rsync']['encrypt_files']:
                        encryptionPassword = self.config['rsync']['encrypt_files']['encryptionPassword']
                    
                    if encryptionPassword:
                        encryptedFile=self.__encryptFiles(source, encryptionPassword, remote)
                        source = encryptedFile

                        #Copying a single file, so make destination a folder
                        destination = os.path.join(self.config['rsync']['destination'],'')
                    else:
                        print "If encrypt_file flag included in rsync, a password must be provided. Aborting rsync." 
                        return
                    
                    if 'remove_source_files' in self.config['rsync']['encrypt_files']:
                        if self.config['rsync']['encrypt_files']['remove_source_files']: remove_source_files = "--remove-source-files"

                else:
                    folder = remote if remote else os.path.basename(source['path'])
                    source = os.path.join(source['path'], '')
                    destination = os.path.join(self.config['rsync']['destination'], folder,'')
        
                if 'pem' in self.config['rsync']:
                    publicKey = "ssh -i %s" % self.config['rsync']['pem']

                print "Backing up %s to remote server %s..." % (source, self.config['rsync']['destination'])
                print "rsync %s -Atrave '%s' %s %s %s" % (remove_source_files,publicKey, ' '.join(str(x) for x in excludes), source, destination)
                print str(excludes)
                result = subprocess.check_output(
                  [
                      'rsync',
                      remove_source_files,
                      '-Atrave',
                      publicKey,
                      source,
                      destination
                  ] + excludes
                )
        
                print result
            else:
                print "No destination specified in rysnc. Aborting rsync."

    ####################################################################################################################
    def __rsyncCleanup(self):
        if 'rsync' in self.config:
            destination = self.config['rsync']['destination']
            self.__cleanup(os.path.dirname(destination), 'rsync')

    ####################################################################################################################
    def __cleanup(self, destination, cleanType):
        # This is a significant rework of the Maelstrom code, the enhancements include: 
        # Enable clean up on the rsync remote folder
        # Enable clean up to roll round the year end and month end
        # Enable specific dates to be retained in the rolling month.
        month = self.config['keep']['month']
        days = self.config['keep']['days']
        dates_to_keep = []
        if 'dates' in self.config['keep']:
            dates_to_keep =  self.config['keep']['dates']
            
        if cleanType == 'rsync':
            if 'keep' in self.config['rsync']:
                if 'month' in self.config['rsync']['keep']: month = self.config['rsync']['keep']['month']
                if 'days' in self.config['rsync']['keep']: days = self.config['rsync']['keep']['days']
                if 'dates' in self.config['rsync']['keep']: dates_to_keep = self.config['rsync']['keep']['dates'] 

        elif cleanType in self.config['projects']:
            project = cleanType
            if 'keep' in self.config['projects'][project]:
                if 'month' in self.config['projects'][project]['keep']:
                    month = self.config['projects'][project]['keep']['month']
                if 'days' in self.config['projects'][project]['keep']:
                    days = self.config['projects'][project]['keep']['days']
                if 'dates' in self.config['projects'][project]['keep']:
                    dates_to_keep = self.config['projects'][project]['keep']['dates'] 
        
        #Clean up year-month folders
        folders = []
        folders.append(os.path.dirname(destination))
        self.__cleanupFolders(folders, month, [])
        
        #Get name of previous months folder. Assume the folder format is yyyy-mm
        year_previous_month = os.path.basename(os.path.normpath(destination))
        if year_previous_month[:-2] == "12":
            year_previous_month = str(year_previous_month[:4]+1)+'-01' #Subtract one from the year and set month to Jan
        else:
            year_previous_month = year_previous_month[:-2]+str(int(year_previous_month[-2:])-1).zfill(2) #Add one to the month

        #Format dates_to_keep as a list of 2 digit dates, add trailing hyphen to each item in dates_to_keep
        dates_to_keep = [str(date).zfill(2)+"-" for date in dates_to_keep] 
        
        #Clean-up days in current and previous month folder. 
        folders = []
        folders.append(destination)
        if os.path.exists(os.path.dirname(destination)+os.sep+year_previous_month):
            folders.append(os.path.dirname(destination)+os.sep+year_previous_month)
        self.__cleanupFolders(folders, days, dates_to_keep)


    ####################################################################################################################
    def __cleanupFolders(self, destination, keep, exclude_folders_begining_with):
        sortedFolders = self.__getSortedFolderList(destination, exclude_folders_begining_with)
        self.__deleteFolders(len(sortedFolders) - keep, sortedFolders)

    ####################################################################################################################
    def __backupFiles(self, files, destination):
        for file in files:
            print "\tBacking up file %s to %s" % (file, destination)
            for fileItem in glob.glob(file):
                if os.path.isfile(fileItem):
                    destinationPath = os.path.join(destination, os.path.dirname(fileItem)[1:])
                    if not os.path.exists(destinationPath):
                        os.makedirs(destinationPath)
                    shutil.copy(fileItem, destinationPath)

    #################################################################################################################### 
    def __backupFolders(self, folders, destination):
        
        for folder_item in folders:
            excludes = []
            if 'folder' in folder_item:
                #Using hierarchical folder structure 
                if 'path' in folder_item['folder']:
                    folder_path = folder_item['folder']['path']
                    
                    if 'excludes' in folder_item['folder']:
                        for exclude in folder_item['folder']['excludes']:
                            if not (os.path.exists(exclude) or os.path.exists(os.path.join(folder_path,exclude))):
                                print "\tExclude path %s not found, check the config entry is correct" % exclude 
                            excludes.append('--exclude=%s' % exclude)
            else:
                #Using simple folder list
                folder_path = folder_item
                
            print "\tBacking up folder %s to %s" % (folder_path, destination)
            filename = "%s.tar.gz" % (os.path.basename(folder_path))
    
            destinationPath = os.path.join(destination, folder_path[1:])
            if not os.path.exists(destinationPath):
                os.makedirs(destinationPath)
            backupFile = os.path.join(destinationPath, filename)
            #print ' '.join(str(x) for x in ["tar", "czfP", backupFile, folder_path] + excludes)
            result = call(["tar", "czfP", backupFile, folder_path] + excludes) 
            if result != 0:
                print "Failed to tar %s" % backupFile

    ####################################################################################################################
    def __backupMongodbs(self, mongodbs, destination):
        #Build the mongodump command based on the config. Config file struture assumes settings are the same for all databases
        mongocommand = 'mongodump --host ' + str(mongodbs['host']) + ' --port ' +  str(mongodbs['port']) + ' '
        if 'usr' in mongodbs and 'pwd' in mongodbs:
            mongocommand += '--username ' + str(mongodbs['usr']) + ' --password ' +  str(mongodbs['pwd']) + ' '
        if 'authenticationDatabase' in mongodbs:
            mongocommand += '--authenticationDatabase ' + str(mongodbs['authenticationDatabase']) + ' '
        if 'sslPEMKeyFile' in mongodbs:
            mongocommand += '--ssl --sslPEMKeyFile ' + str(mongodbs['sslPEMKeyFile']) + ' '
        output_type = '--archive=' if ('output' in mongodbs and 'archive' == mongodbs['output']) else '--out='
        output_type += destination
        #Run command for each database in the config file
        for mongodb in mongodbs['names']:
            print "\tBacking up mongodb %s to %s" % (mongodb, destination)
            self.__backupMongodb(mongodb, mongocommand, output_type)

    ####################################################################################################################
    def __backupMongodb(self, mongodb, mongocommand, output_type):
        #Complete the database specific commands
        if output_type[:9] == "--archive":
            output_type += os.sep + mongodb + '.tar.gz '
        else:
            output_type += os.sep + mongodb + ' '
        mongocommand += output_type + ' --gzip ' + ' --db ' + mongodb + ' '
        #Convert command string to list
        safe_args = shlex.split(mongocommand)
        #Execute os command
        subprocess.check_output(safe_args)    

    ####################################################################################################################
    def __backupDatabases(self, databases, destination):
        if 'prefix' in databases:
            names = self.__listDatabases(databases['prefix'], databases['usr'], databases['pwd'])
        else:
            names = databases['names']

        for database in names:
            self.__backupDatabase(database, destination, databases['usr'], databases['pwd'])

    ####################################################################################################################
    def __listDatabases(self, prefix, usr, pwd):
        matchingCommand = "SHOW DATABASES LIKE '" + prefix + "'"
        listCommand = ["mysql", "-u", usr, "-p" + pwd, "-B", "-N", "-e", matchingCommand]
        listProcess = subprocess.Popen(listCommand, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        listOutput = listProcess.communicate()[0]
        return listOutput.rstrip().split('\n')

    ####################################################################################################################
    def __backupDatabase(self, database, destination, usr, pwd):
        print "\tBacking up database %s to %s" % (database, destination)
        filename = "%s.sql.gz" % (os.path.basename(database))
        backupFile = os.path.join(destination, filename)

        dumpCommand = ["mysqldump", "-u", usr, "-p" + pwd, database]
        dumpProcess = subprocess.Popen(dumpCommand, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        dumpOutput = dumpProcess.communicate()[0]

        zipFile = gzip.open(backupFile, "wb")
        zipFile.write(dumpOutput)
        zipFile.close()

    ####################################################################################################################
    def __encryptFiles(self, source, password, remote=None):
        '''
        Encrypts a file using gpg. If the source is a folder it is archived to a sinlge file before encrypting
        '''
        print "\Encrypting Files %s " % (source['path'])

        if os.path.isfile(source['path']):
            archiveRequired = False
            fileToEncrypt = source['path']
            encryptedFile = fileToEncrypt + ".gpg"
        else:
            archiveRequired = True
            folderToArchive = source['path']
            if remote:
                remote = os.sep + str(remote)
            else:
                remote = ""
            encryptedFile = source['path'] + str(remote) + ".tar.gz.gpg"
        
        #Delete the file if it already exists
        try:
            os.remove(encryptedFile)
        except OSError:
            pass            

        if archiveRequired:
            excludes = []
            if 'excludes' in source:
                for exclude in source['excludes']:
                    excludes.append("--exclude='" + folderToArchive + exclude + "' ")

            #tar is picky about the position of the Exclude list, see 
            #https://www.linuxquestions.org/questions/showthread.php?threadid=194476&highlight=exclude+directories+recursively+tar
            compressCommand = "tar --create --gzip " + " ".join(excludes) + " " + folderToArchive + " --file - " 
            compressSafeArgs = shlex.split(compressCommand)
            
            #Create two processes (one to archive and one to encrypt) pass the stdout of the archive as the stdin of the encrypt
            compressProcess = subprocess.Popen(compressSafeArgs, stdout=subprocess.PIPE)
            encryptProcess = subprocess.Popen(["gpg", "--symmetric", "-o", encryptedFile, "--batch", "--passphrase", str(password)], stdin=compressProcess.stdout, stdout=subprocess.PIPE)
            compressProcess.stdout.close()  # Allow compressProcess to receive a SIGPIPE if encryptProcess exits.
        else:
            #create a single process to encrypt
            encryptCommand = ["gpg", "--symmetric", "--batch", "--passphrase", password, fileToEncrypt]
            encryptProcess = subprocess.Popen(encryptCommand, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        encryptOutput = encryptProcess.communicate()[0]    
        return encryptedFile
    
    ####################################################################################################################
    def __deleteFolders(self, deleteCount, sortedFolders):
        if deleteCount > 0:
            foldersToDelete = sortedFolders[:deleteCount]
            for folder in foldersToDelete:
                print "\tDeleting %s" % folder[0]
                shutil.rmtree(folder[0])

    ####################################################################################################################
    def __getSortedFolderList(self, destinations, ignore_folders_begining_with):
        #Build a folder list and sort by last modified date. 
        #Excluded any folder from the list if they begin with any of the values in ignore_folders_begining_with
        dir_file_date_list = []
        for destination in destinations:
            for folder in os.listdir(destination):
                #Check if folder begins with a value that we want to ignore
                bln_ignore = False
                for ignore in ignore_folders_begining_with:
                    if folder[:len(ignore)] == ignore:
                        bln_ignore = True
                        break
                
                #Add the folder to the list
                if not bln_ignore:
                    file_attributes = [os.path.join(destination, folder),folder, os.path.getmtime(os.path.join(destination, folder))]
                    dir_file_date_list.append(file_attributes)
        
        dir_file_date_list.sort(key=lambda x: x[2]) #Sort by third item (date)
        return dir_file_date_list

    ####################################################################################################################
    def __createBackupFolder(self, path):
        if not os.path.exists(path):
            os.makedirs(path)


####################################################################################################################
# S C R I P T    M A I N    E N T R Y
####################################################################################################################

if __name__ == "__main__":
    ObibaBackup().run()
