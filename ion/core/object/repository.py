#!/usr/bin/env python


"""
@file ion/core/object/repository.py
@brief Repository for managing data structures
@author David Stuebe
@author Matt Rodriguez

TODO
Refactor Merge to use a proxy repository for the readonly objects - they must live in a seperate workspace.

"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

import sys

import time
from twisted.internet import threads, reactor, defer

from ion.core.object import gpb_wrapper
from ion.core.object import object_utils

from ion.util import procutils as pu

commit_type = object_utils.create_type_identifier(object_id=8, version=1)
mutable_type = object_utils.create_type_identifier(object_id=6, version=1)
branch_type = object_utils.create_type_identifier(object_id=5, version=1)

from ion.core.object import object_utils

class RepositoryError(Exception):
    """
    An exception class for errors in the object management repository 
    """

class Repository(object):
    
    UPTODATE='up to date'
    MODIFIED='modified'
    NOTINITIALIZED = 'This repository is not initialized yet (No commit checked out)'

    CommitClassType = object_utils.create_type_identifier(object_id=8, version=1)
    LinkClassType = object_utils.create_type_identifier(object_id=3, version=1)
    
    def __init__(self, head=None):
        
        
        #self.status  is a property determined by the workspace root object status
        
        self._object_counter=1
        """
        A counter object used by this class to identify content objects untill
        they are indexed
        """
        
        self._workspace = {}
        """
        A dictionary containing objects which are not yet indexed, linked by a
        counter refrence in the current workspace
        """
        
        self._workspace_root = None
        """
        Pointer to the current root object in the workspace
        """
        
        
        self._commit_index = {}
        """
        A dictionary containing the commit objects - all immutable content hashed
        """
        
        self._hashed_elements = None
        """
        All content elements are stored here - from incoming messages and
        new commits - everything goes here. Now it can be decoded for a checkout
        or sent in a message.
        """
        
        
        self._current_branch = None
        """
        The current branch object of the mutable head
        Branch names are generallly nonsense (uuid or some such)
        """
        
        self.branchnicknames = {}
        """
        Nick names for the branches of this repository - these are purely local!
        """
        
        self._detached_head = False
        
        
        self._merge_from = []
        """
        Keep track of branches which were merged into this one!
        Like _current_brach, _merge_from is a list of links - not the actual
        commit refs
        """
        
        self._merge_root=[]
        """
        When merging a repository state there are multiple root object in a
        read only state from which you can draw values using repo.Merging[ind].[field]
        """
        
        self._stash = {}
        """
        A place to stash the work space under a saved name.
        """
        
        self._workbench=None
        """
        The work bench which this repository belongs to...
        """
        
        self._upstream={}
        """
        The upstream source of this repository. 
        """
        
        if head:
            self._dotgit = self._load_element(head)
            # Set it to modified and give it a new ID as soon as we get it!
            self._dotgit.Modified = True
            self._dotgit.MyId = self.new_id()
        else:
           
            mutable_cls = object_utils.get_gpb_class_from_type_id(mutable_type)
            self._dotgit = self._create_wrapped_object(mutable_cls, addtoworkspace = False)
            self._dotgit.repositorykey = pu.create_guid()
        """
        A specially wrapped Mutable GPBObject which tracks branches and commits
        It is not 'stored' in the index - it lives in the workspace
        """
    
    def __repr__(self):
        output  = '============== Repository (status: %s) ==============\n' % self.status
        output += str(self._dotgit) + '\n'
        output += '============== Root Object ==============\n'
        output += str(self._workspace_root) + '\n'
        output += '============ End Resource ============\n'
        return output
    
    @property
    def repository_key(self):
        return self._dotgit.repositorykey
    
    @property
    def merge_objects(self):
        return self._merge_root
    
    
    def _set_upstream(self, source):
        self._upstream = source
        
    def _get_upstream(self):
        return self._upstream
    
    upstream = property(_get_upstream, _set_upstream)
    
    def _get_root_object(self):
        return self._workspace_root
        
    def _set_root_object(self, value):
        if not isinstance(value, gpb_wrapper.Wrapper):
            raise RepositoryError('Can not set the root object of the repository to a value which is not an instance of Wrapper')
    
        if not value.Repository is self:
            value = self._copy_from_other(value)
        
        if not value.MyId in self._workspace:
            self._workspace[value.MyId] = value
            
        self._workspace_root = value
        
        
    root_object = property(_get_root_object, _set_root_object)
    
    @property
    def branches(self):
        """
        Convience method to access the branches from the mutable head (dotgit object)
        """
        return self._dotgit.branches
    
    @property
    def commit_head(self):
        """
        Convience method to access the current commit 
        """
        if self._detached_head:
            log.warn('This repository is currently a detached head. The current commit is not at the head of a branch.')
        
        if len(self._current_branch.commitrefs) == 1:          
            return self._current_branch.commitrefs[0]
        else:
            raise RepositoryError('Branch should merge on read. Invalid state with more than one commit at the head of a branch!')
    
    def branch(self, nickname=None):
        """
        @brief Create a new branch from the current commit and switch the workspace to the new branch.
        """
        ## Need to check and then clear the workspace???
        #if not self.status == self.UPTODATE:
        #    raise Exception, 'Can not create new branch while the workspace is dirty'
        
        if self._current_branch != None and len(self._current_branch.commitrefs)==0:
            # Unless this is an uninitialized repository it is an error to create
            # a new branch from one which has no commits yet...
            raise RepositoryError('The current branch is empty - a new one can not be created untill there is a commit!')
        
        
        brnch = self.branches.add()    
        brnch.branchkey = pu.create_guid()
        
        if nickname:
            if nickname in self.branchnicknames.keys():
                raise RepositoryError('That branch nickname is already in use.')
            self.branchnicknames[nickname]=brnch.branchkey

        if self._current_branch:
            # Get the linked commit
            
            if len(brnch.commitrefs)>1:
                raise RepositoryError('Branch should merge on read. Invalid state!')
            elif len(brnch.commitrefs)==1:                
                cref = self._current_branch.commitrefs[0]
            
                bref = brnch.commitrefs.add()
            
                # Set the new branch to point at the commit
                bref.SetLink(cref)
            
            
            # Making a new branch re-attaches to a head!
            if self._detached_head:
                self._workspace_root.SetStructureReadWrite()
                self._detached_head = False
                
        self._current_branch = brnch
        return brnch.branchkey
    
    def remove_branch(self,name):
        branchkey = self.branchnicknames.get(name,None)
        if not branchkey:
            branchkey = name
        
        for idx, item in zip(range(len(self.branches)), self.branches):
            if item.branchkey == branchkey:
                del self.branches[idx]
                break
        else:
            log.info('Branch %s not found!' % name)
        
    def get_branch(self, name):
        
        branchkey = self.branchnicknames.get(name,None)
        if not branchkey:
            branchkey = name
        
        branch = None
        for item in self.branches:
            if item.branchkey == branchkey:
                branch = item
                break
        else:
            log.info('Branch %s not found!' % name)
            
        return branch
    
    @defer.inlineCallbacks
    def checkout(self, branchname=None, commit_id=None, older_than=None):
        """
        Check out a particular branch
        Specify a branch, a branch and commit_id or a date
        Branch can be either a local nick name or a global branch key
        """
        
        if self.status == self.MODIFIED:
            raise RepositoryError('Can not checkout while the workspace is dirty')
            #What to do for uninitialized? 
        
        #Declare that it is a detached head!
        detached = False
        
        if older_than and commit_id:
            raise RepositoryError('Checkout called with both commit_id and older_than!')
        
        if not branchname:
            raise RepositoryError('Checkout must specify a branchname!')
            
            
        branch = self.get_branch(branchname)
        if not branch:
            raise RepositoryError('Branch Key: "%s" does not exist!' % branchname)
            
        if len(branch.commitrefs)==0:
            raise RepositoryError('This branch is empty - there is nothing to checkout!')
            
            
        # Set the current branch now!
        self._current_branch = branch
        
        cref = None
            
        if commit_id:
            
            # IF you are checking out a specific commit ID it is always a detached head!
            detached = True
            
            # Use this set to make sure we only examine each commit once!
            touched_refs = set()
                
            crefs = branch.commitrefs[:]
            
            while len(crefs) >0:
                new_set = set()
                    
                for ref in crefs:
                        
                    if ref.MyId == commit_id:
                        
                        # Empty the crefs set to exit the while loop!
                        crefs = set()
                        # Save the CRef!
                        cref = ref
                        break # Break to ref in crefs
                        
                    # For each child reference...
                    for pref in ref.parentrefs:
                        # If we have not already looked at this one...
                        p_commit = pref.commitref
                        if not p_commit in touched_refs:                            
                            new_set.add(p_commit)
                            touched_refs.add(p_commit)    
                    
                else:
                    crefs = new_set                    
            else:
                if not cref:
                    raise RepositoryError('End of Ancestors: No matching reference \
                                          found in commit history on branch name %s, \
                                          commit_id: %s' % (branch_name, commit_id))
                
            
            
        elif older_than:
            
            # IF you are checking out a specific commit date it is always a detached head!
            detached = True
            
            # Need to make sure we get the closest commit to the older_than date!
            younger_than = -9999.99
            
            # Use this set to make sure we only examine each commit once!
            touched_refs = set()
            
            crefs = branch.commitrefs[:]
            
            while len(crefs) >0:
                
                new_set = set()
                
                for ref in crefs:
                        
                    if ref.date <= older_than & ref.date > younger_than:
                        cref = ref
                        younger_than = ref.date
                        
                    # Only keep looking at parent references if they are to young
                    elif ref.date > older_than:
                        # For each child reference...
                        for pref in ref.parentrefs:
                            # If we have not already looked at this one...
                            if not pref in touched_refs:                            
                                new_set.add(pref)
                                touched_refs.add(pref)    
                       
                crefs = new_set
                       
            else:
                if not cref:
                    raise RepositoryError('End of Ancestors: No matching commit \
                                          found in commit history on branch name %s, \
                                          older_than: %s' % (branch_name, older_than))
                
        # Just checking out the current head - need to make sure it has not diverged! 
        else:
            
            if len(branch.commitrefs) ==1:
                
                cref = branch.commitrefs[0]
                
            else:
                log.warn('BRANCH STATE HAS DIVERGED - MERGING') 
                
                cref = self.merge_by_date(branch)
                
        # Do some clean up!
        for item in self._workspace.itervalues():
            #print 'ITEM',item
            item.Invalidate()
        self._workspace = {}
        self._workspace_root = None
            
        # Automatically fetch the object from the hashed dictionary
        rootobj = yield self.get_remote_linked_object(cref.GetLink('objectroot'))
        self._workspace_root = rootobj
        
        yield self._load_remote_links(rootobj)
        
        
        self._detached_head = detached
        
        if detached:
            branch_cls = object_utils.get_gpb_class_from_type_id(branch_type)
            self._current_branch = self._create_wrapped_object(branch_cls, addtoworkspace=False)
            bref = self._current_branch.commitrefs.add()
            bref.SetLink(cref)
            self._current_branch.branchkey = 'detached head'
            
            rootobj.SetStructureReadOnly()
            
        defer.returnValue(rootobj)
        return
        
        
    def merge_by_date(self, branch):
        
        crefs=branch.commitrefs[:]
        
        newest = -999.99
        for cref in crefs:
            if cref.date > newest:
                head_cref = cref
                newest = cref.date
            
        # Deal with the newest ref seperately
        crefs.remove(head_cref)
            
        # make a new commit ref
        commit_cls = object_utils.get_gpb_class_from_type_id(commit_type)
        cref = self._create_wrapped_object(commit_cls, addtoworkspace=False)
                    
        cref.date = pu.currenttime()

        pref = cref.parentrefs.add()
        pref.SetLinkByName('commitref',head_cref)
        pref.relationship = pref.Relationship.PARENT

        cref.SetLinkByName('objectroot', head_cref.objectroot)

        cref.comment = 'Merged divergent branch by date keeping the newest value'

        for ref in crefs:
            pref = cref.parentrefs.add()
            pref.SetLinkByName('commitref',ref)
            pref.relationship = pref.Relationship.MERGEDFROM
        
        structure={}                            
        # Add the CRef to the hashed elements
        cref.RecurseCommit(structure)
        
        # set the cref to be readonly
        cref.ReadOnly = True
        
        # Add the cref to the active commit objects - for convienance
        self._commit_index[cref.MyId] = cref

        # update the hashed elements
        self._hashed_elements.update(structure)
        
        del branch.commitrefs[:]
        bref = branch.commitrefs.add()
        bref.SetLink(cref)
        
        return cref
        
    def reset(self):
        
        if self.status != self.MODIFIED:
            # What about not initialized
            return
        
        if len(self._current_branch.commitrefs)==0:
            raise RepositoryError('This current branch is empty - there is nothing to reset too!')
        
        cref = self._current_branch.commitrefs[0]

        # Do some clean up!
        for item in self._workspace.itervalues():
            item.Invalidate()
        self._workspace = {}
        self._workspace_root = None
            
            
        # Automatically fetch the object from the hashed dictionary or fetch if needed!
        rootobj = cref.objectroot
        self._workspace_root = rootobj
        
        self._load_links(rootobj)
                
        return rootobj
        
        
    def commit(self, comment=''):
        """
        Commit the current workspace structure
        """
        
        # If the repo is in a valid state - make the commit even if it is up to date
        if self.status == self.MODIFIED or self.status == self.UPTODATE:
            structure={}
            self._workspace_root.RecurseCommit(structure)
                                
            cref = self._create_commit_ref(comment=comment)
                
            # Add the CRef to the hashed elements
            cref.RecurseCommit(structure)
            
            # set the cref to be readonly
            cref.ReadOnly = True
            
            # Add the cref to the active commit objects - for convienance
            self._commit_index[cref.MyId] = cref

            # update the hashed elements
            self._hashed_elements.update(structure)
            log.info('Commited repository - Comment: cref.comment')
                            
        else:
            raise RepositoryError('Repository in invalid state to commit')
        
        # Like git, return the commit id
        branch = self._current_branch
        return branch.commitrefs.GetLink(0).key
            
            
    def _create_commit_ref(self, comment='', date=None):
        """
        @brief internal method to create commit references
        @param comment a string that describes this commit
        @param date the date to associate with this commit. If not given then 
        the current time is used.
        @retval a string which is the commit reference
        """
        # Now add a Commit Ref
        # make a new commit ref
        commit_cls = object_utils.get_gpb_class_from_type_id(commit_type)
        cref = self._create_wrapped_object(commit_cls, addtoworkspace=False)
        
        if not date:
            date = pu.currenttime()
            
        cref.date = date
            
        branch = self._current_branch
            
        # If this is the first commit to a new repository the current branch is a dummy
        # If it is initialized it is real and we need to link to it!
        if len(branch.commitrefs)==1:
            
            # This branch is real - add it to our ancestors
            pref = cref.parentrefs.add()
            parent = branch.commitrefs[0] # get the parent commit ref
            pref.SetLinkByName('commitref',parent)
            pref.relationship = pref.Relationship.PARENT
        elif len(branch.commitrefs)>1:
            raise RepositoryError('The Branch is in an invalid state and should have been merged on read!')
        else:
            # This is a new repository and we must add a place for the commit ref!
            branch.commitrefs.add()
            # Since the commit has no parents, set the root_seed
            cref.root_seed = self.repository_key
        
        
        # For each branch that we merged from - add a  reference
        for mrgd in self._merge_from:
            pref = cref.parentrefs.add()
            pref.SetLinkByName('commitref',mrgd)
            pref.relationship = pref.Relationship.MERGEDFROM
            
        cref.comment = comment
        cref.SetLinkByName('objectroot', self._workspace_root)            
        
        # Clear the merge root and merged from
        self._merge_from = []
        self._merge_root = []
        
        # Update the cref in the branch
        branch.commitrefs.SetLink(0,cref)
        
        return cref
            

    @defer.inlineCallbacks
    def merge(self, branchname=None, commit_id = None):
        """
        merge the named branch in to the current branch
        
        This method does not 'do' the merger of state.
        It simply adds the parent ref to the repositories merged from list!
        
        """
        
        if self.status == self.MODIFIED:
            log.warn('Merging while the workspace is dirty better to make a new commit first!')
            #What to do for uninitialized?
            
        if self.status == self.NOTINITIALIZED:
            raise RepositoryError('Can not merge in a repository which is not initialized (Checkout something first!)')
        
        crefs=[]
                
        if commit_id:
            if commit_id == self._current_branch.commitrefs[0].MyId:
                raise RepositoryError('Can not merge into self!')
            try:
                crefs.append(self._commit_index[commit_id])
            except KeyError, ex:
                raise RepositoryError('Can not merge from unknown commit_id %s' % commit_id)
        
        elif branchname:
            
            branch = self.get_branch(branchname)
            if not branch:
                raise RepositoryError('Branch Key: "%s" does not exist!' % branchname)
            
            if branch.branchkey == self._current_branch.branchkey:
                if len(branch.commitrefs)<2:
                    raise RepositoryError('Can not merge with current branch head (self into self)')
                
                # Merge the divergent states of this branch!
                crefs = branch.commitrefs
                crefs.pop(self._current_branch.commitrefs[0])
                
            else:
                # Assume we merge any and all states of this branch?
                crefs.extend( branch.commitrefs)
        
        else:
            log.debug('''Arguments to Repository.merge - branchname: %s; commit_id: %s''' \
                      % (branchname, commit_id))
            raise RepositoryError('merge takes either a branchname argument or a commit_id argument!')
        
        assert len(crefs) > 0, 'Illegal state reached in Repository Merge function!'
        
        for cref in crefs:
            self._merge_from.append(cref)
            
            rootobj = yield self.get_remote_linked_object(cref.GetLink('objectroot'))
            merge_root = rootobj
            merge_root.ReadOnly = True
            # The child objects inherit there ReadOnly setting from the root
            yield self._load_remote_links(rootobj)
            
            self._merge_root.append(merge_root)
        
        
        
    @property
    def status(self):
        """
        Check the status of the current workspace - return a status
          up to date
          changed
        """
        
        if self._workspace_root:
            if self._workspace_root.Modified:
                return self.MODIFIED
            else:
                return self.UPTODATE
        else:
            return self.NOTINITIALIZED
        
        
    def log_commits(self,branchname=None):
        
        if branchname == None:
            branchname = self._current_branch.branchkey
        
        branch = self.get_branch(branchname)
        log.info('$$ Logging commits on Branch %s $$' % branchname)
        cntr = 0
        for cref in branch.commitrefs:
            cntr+=1
            log.info('$$ Branch Head Commit # %s $$' % cntr)
            
            log.info('Commit: \n' + str(cref))
        
            while len(cref.parentrefs) >0:
                for pref in cref.parentrefs:
                    if pref.relationship == pref.Relationship.PARENT:
                            cref = pref.commitref
                            log.info('Commit: \n' + str(cref))
                            break # There should be only one parent ancestor from a branch
                
        
    def stash(self, name):
        """
        Stash the current workspace for later reference
        """
        raise Exception('Not implemented yet')
        
    def create_object(self, type_id):
        """
        @brief CreateObject is used to make new locally create objects which can
        be added to the resource's data structure.
        @param type_id is the type_id of the object to be created
        @retval the new object which can now be attached to the resource
        """
        
        cls = object_utils.get_gpb_class_from_type_id(type_id)
        obj = self._wrap_message_object(cls())
        return obj
        
        
    def _create_wrapped_object(self, rootclass, obj_id=None, addtoworkspace=True):        
        """
        Factory method for making wrapped GPB objects from the repository
        """
        message = rootclass()
            
        obj = self._wrap_message_object(message, obj_id, addtoworkspace)
            
        return obj
        
    def _wrap_message_object(self, message, obj_id=None, addtoworkspace=True):
        
        if not obj_id:
            obj_id = self.new_id()
        obj = gpb_wrapper.Wrapper(message)
        obj._repository = self
        obj._root = obj
        obj._parent_links = set()
        obj._child_links = set()
        obj._derived_wrappers={}
        obj._read_only = False
        obj._myid = obj_id
        obj._modified = True
        obj._invalid = False

        if addtoworkspace:
            self._workspace[obj_id] = obj
                        
        return obj
        
    def new_id(self):
        """
        This id is a purely local concern - not used outside the local scope.
        """
        self._object_counter += 1
        return str(self._object_counter)
     
    def get_linked_object(self, link):
                
        if link.ObjectType != self.LinkClassType:
            raise RepositoryError('Illegal argument type in get_linked_object.')
                
                
        if not link.IsFieldSet('key'):
            return None
                
        if self._workspace.has_key(link.key):
            
            obj = self._workspace.get(link.key)
            # Make sure the parent is set...
            #self.set_linked_object(link, obj)
            obj.AddParentLink(link)
            return obj

        elif self._commit_index.has_key(link.key):
            # make sure the parent is set...
            obj = self._commit_index.get(link.key)
            obj.AddParentLink(link)
            return obj

        elif self._hashed_elements.has_key(link.key):
            
            element = self._hashed_elements.get(link.key)
            
        else:
            
            log.debug('Linked object not found. Need non local object: %s' % str(link))
            
            raise KeyError('Object not found in the local work bench.')
            
            
            
        if not link.type.object_id == element.type.object_id and \
                link.type.version == element.type.version:
            raise RepositoryError('The link type does not match the element type found!')
            
        obj = self._load_element(element)
            
        # For objects loaded from the hash the parent child relationship must be set
        #self.set_linked_object(link, obj)
        obj.AddParentLink(link)
        
        if obj.ObjectType == self.CommitClassType:
            self._commit_index[obj.MyId]=obj
            obj.ReadOnly = True
                
        elif link.Root.ObjectType == self.CommitClassType:
            # if the link is a commit but the linked object is not then it is a root object
            # The default for a root object should be ReadOnly = False
            self._workspace[obj.MyId]=obj
            obj.ReadOnly = False
            
        else:
            # When getting an object from it's parent, use the parents readonly setting
            self._workspace[obj.MyId]=obj
            obj.ReadOnly = link.ReadOnly
            
        return obj
            
    @defer.inlineCallbacks
    def get_remote_linked_object(self, link):
            
        try:
            obj = self.get_linked_object(link)
        except KeyError, ex:
            log.info('"get_remote_linked_object": Caught object not found:'+str(ex))
            res = yield self._fetch_remote_objects([link,])
            # Object is now in the hashed objects dictionary
            obj = self.get_linked_object(link)
        
        defer.returnValue(obj)
        return 
        
         
    @defer.inlineCallbacks
    def _fetch_remote_objects(self, links):
    
        if not self._workbench:
                raise RepositoryError('Object not found and not work bench is present!')
                
        proc = self._workbench._process
        # Check for Iprocess once defined outside process module?
        if not proc:
            raise RepositoryError('Linked Obect not found and work bench has no process to get it with!')
            
        if hasattr(proc, 'fetch_linked_objects'):
            # Get the method from the process if it overrides workbench
            fetch_linked_objects = proc.fetch_linked_objects
        else:
            fetch_linked_objects = self._workbench.fetch_linked_objects
            
        #@TODO provide catch mechanism to use the service name instead of the
        # process name if the process does not respond...
        result = yield fetch_linked_objects(self.upstream['process'], links)
        
        defer.returnValue(result)
            
            
    def _load_links(self, obj):
        """
        Load the child objects into the work space
        """
        for link in obj.ChildLinks:
            child = self.get_linked_object(link)  
            self._load_links(child)
            

    @defer.inlineCallbacks
    def _load_remote_links(self, obj):
        """
        Load links which may require remote (deferred) access
        """
        remote_objects = []
        for link in obj.ChildLinks:
            try:
                child = self.get_linked_object(link)
                yield self._load_remote_links(child)
            except KeyError, ex:
                log.info('"_load_remote_links": Caught object not found:'+str(ex))
                remote_objects.append(link)
            
            
        if remote_objects:
            res = yield self._fetch_remote_objects(remote_objects)
            res = yield self._load_remote_links(obj)
        defer.returnValue(True)
            
    def _load_element(self, element):
        
        #log.debug('_load_element' + str(element))
        
        # check that the caluclated value in element.sha1 matches the stored value
        if not element.key == element.sha1:
            raise RepositoryError('The sha1 key does not match the value. The data is corrupted! \n' +\
            'Element key %s, Calculated key %s' % (object_utils.sha1_to_hex(element.key), object_utils.sha1_to_hex(element.sha1)))
        
        cls = object_utils.get_gpb_class_from_type_id(element.type)
                                
        # Do not automatically load it into a particular space...
        obj = self._create_wrapped_object(cls, obj_id=element.key, addtoworkspace=False)
            
        # If it is a leaf element set the bytes for the object, do not load it
        # If it is not a leaf element load it and find its child links
        if element.isleaf:
            
            obj._bytes = element.value
            
        else:
            obj.ParseFromString(element.value)
            obj.FindChildLinks()


        obj.Modified = False
        
        # Make a note in the element of the child links as well!
        for child in obj.ChildLinks:
            element.ChildLinks.add(child.key)
        
        return obj
        
        
    def _copy_from_other(self, value):
        """
        Copy an object from another repository. This method will serialize any
        content which is not already in the hashed objects. Then read it back in
        as new objects in this repository. The objects must not already exist in
        the current repository or it will return an error
        """
        if not isinstance(value, gpb_wrapper.Wrapper):
            raise RepositoryError('Can not copy an object which is not an instance of Wrapper')    
        
        if not value.IsRoot:
            # @TODO provide for transfer by serialization and re instantiation
            raise RepositoryError('You can not copy only part of a gpb composite, only the root!')
        
        if value.Repository is self:
            raise RepositoryError('Can not copy an object from the same repository')
            
        if value.ObjectType.object_id <= 1000 and value.ObjectType.object_id != 4:
            # This is a core object other than an IDRef to another repository.
            # Generally this should not happen...
            log.warn('Copying core objects is not an error but unexpected results may occur.')
            
        structure={}
        value.RecurseCommit(structure)
        self._hashed_elements.update(structure)
        
        # Get the element by creating a temporary link and loading links...
        link_cls = object_utils.get_gpb_class_from_type_id(self.LinkClassType)
        link = self._create_wrapped_object(link_cls, addtoworkspace=False)
        link.key = value.MyId
        object_utils.set_type_from_obj(value, link.type)
        
        self._load_links(link)
        
        obj = self.get_linked_object(link)
        
        # Get rid of the temporary link
        obj.ParentLinks.discard(link)
        
        return obj
    
    
        
    def set_linked_object(self,link, value):        
        # If it is a link - set a link to the value in the wrapper
        if link.ObjectType != link.LinkClassType:
            raise RepositoryError('Can not set a composite field unless it is of type Link')
                    
        if not value.IsRoot == True:
            # @TODO provide for transfer by serialization and re instantiation
            raise RepositoryError('You can not set a link equal to part of a gpb composite, only the root!')
            
        
        # if this value is from another repository... you need to load it from the hashed objects into this repository
        if not value.Repository is self:
            
            value = self._copy_from_other(value)
        
        
        if link.key == value.MyId:
                # Add the new link to the list of parents for the object
                value.AddParentLink(link) 
                # Setting it again is a pass...
                return
        
        if link.InParents(value):
            raise RepositoryError('You can not create a recursive structure - this value is also a parent of the link you are setting.')

        # Add the new link to the list of parents for the object
        value.AddParentLink(link) 
        
        # If the link is currently set
        if link.key:
                            
            old_obj = self._workspace.get(link.key,None)
            if old_obj:
                plinks = old_obj.ParentLinks
                plinks.remove(link)
                
                
                # Don't do this - read below!
                # If there are no parents left for the object delete it
                #if len(plinks)==0:
                    
                    # This could lead to an invalid state
                    #del self._workspace[link.key]
                    
                    # to do this correctly make a weak reference
                    # Remove it from the work space
                    # del the old_obj
                    # if it is still there in the weak ref put it back
                    # because something is still referenceing it
                    
                    # But really who cares - just leaving it hanging in the
                    # workbench, it will be garbage collected later.
                
        else:
            
            #Make sure the link is in the objects set of child links
            link.ChildLinks.add(link) # Adds to the links root wrapper!
            
        
            
        # Set the id of the linked wrapper
        link.key = value.MyId
        
        # Set the type
        tp = link.type
        object_utils.set_type_from_obj(value, tp)
        #link.type = object_utils.get_type_from_obj(value)
            
    