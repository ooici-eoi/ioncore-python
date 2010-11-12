#!/usr/bin/env python


"""
@Brief Repository for managing data structures
@author David Stuebe
@author Matt Rodriguez

TODO
Remove name from branch
Create ancestor iterator
Create pretty print for ancestors

"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

import sys

from net.ooici.core.mutable import mutable_pb2

from twisted.internet import defer

from ion.core.object import gpb_wrapper

from ion.util import procutils as pu

from net.ooici.core.type import type_pb2
from net.ooici.core.link import link_pb2


class Repository(object):
    
    UPTODATE='up to date'
    MODIFIED='modified'
    NOTINITIALIZED = 'This repository is not initialized yet'

    CommitClassType = gpb_wrapper.set_type_from_obj(mutable_pb2.CommitRef())
    LinkClassType = gpb_wrapper.set_type_from_obj(link_pb2.CASRef())
    
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
        
        
        self._merged_from = []
        """
        Keep track of branches which were merged into this one!
        Like _current_brach, _merged_from is a list of links - not the actual
        commit refs
        """
        
        self._stash = {}
        """
        A place to stash the work space under a saved name.
        """
        
        self._workbench=None
        """
        The work bench which this repository belongs to...
        """

        if head:
            self._dotgit = self._load_element(head)
        else:
           
            self._dotgit = self.create_wrapped_object(mutable_pb2.MutableNode, addtoworkspace = False)
            self._dotgit.repositorykey = pu.create_guid()
        """
        A specially wrapped Mutable GPBObject which tracks branches and commits
        It is not 'stored' in the index - it lives in the workspace
        """
    
    @property
    def repository_key(self):
        return self._dotgit.repositorykey
    
    @property
    def branches(self):
        return self._dotgit.branches
        
        
    def checkout(self, branch=None, commit_id=None, older_than=None):
        """
        Check out a particular branch
        Specify a branch, a branch and commit_id or a date
        Branch can be either a local nick name or a global branch key
        """
        
        if self.status == self.MODIFIED:
            raise Exception, 'Can not checkout while the workspace is dirty'
            #What to do for uninitialized? 
        
        #Declare that it is a detached head!
        detached = False
        
        if older_than and commit_id:
            raise Exception, 'Checkout called with both commit_id and older_than!'
        
        
        if branch:
                        
            branchkey = self.branchnicknames.get(branch,None)
            if not branchkey:
                branchkey = branch
            
            for item in self._dotgit.branches:
                if item.branchkey == branchkey:
                    branch = item
                    head_ref = item.commitref # THIS WILL LOAD THE CREF!
                    cref = head_ref
                    break
            else:
                raise Exception, 'Branch Key: %s, does not exist!' % branchkey
            
            if commit_id:
                
                if head_ref.myid == commit_id:
                    cref = head_ref
                else:
                    
                    detached = True
                    ref = head_ref 
                    while len(ref.parentrefs) >0:
                        for pref in ref.parentrefs:
                            if pref.relationship == pref.parent:
                                ref = pref.commitref
                                break # There should be only one parent ancestor from a branch
                        else:
                            raise Exception, 'End of Branch: No parent found on branch name: %s, commit_id: %s' % (branch_name, commit_id)
                        if ref.myid == commit_id:
                            cref = ref
                            break
                    else:
                        raise Exception, 'End of Ancestors: No matching reference found in commit history on branch name %s, commit_id: %s' % (branch_name, commit_id)
                        
                
                
            elif older_than:
                
                if head_ref.date <= older_than:
                    cref = head_ref
                    # Not sure this is really the spirit of the thing?
                else:
                    
                    detached = True
                    ref = head_ref 
                    while len(ref.parentrefs) >0:
                        for pref in ref.parentrefs:
                            if pref.relationship == pref.Parent:
                                ref = pref.commitref
                                break # There should be only one ancestor from a branch
                        else:
                            raise Exception, 'End of Branch: No matching ancestor found on branch name: %s, older_than: %s' % (branch_name, older_than)
                        if ref.date <= older_than:
                            cref = ref
                            break
                    else:
                        raise Exception, 'End of Ancestors: No matching commit found in commit history on branch name %s, older_than: %s' % (branch_name, older_than)
                        
        elif commit_id:
            
            # This is dangerous, but lets do it anyway - for now!
            if self._hashed_elements.has_key(commit_id):
                element = self._hashed_elements[commit_id]
                cref = self._load_element(element)
                
                detached = True
            else:
                # Check more places? Ask for it from the repository?
                raise Exception, 'Can not checkout an id that does not exist!'
            
        else:
            raise Exception, 'Checkout must specify a branch or a commit_id'
        
        # Do some clean up!
        self._workspace = {}
        self._workspace_root = None
            
            
        # Automatically fetch the object from the hashed dictionary or fetch if needed!
        rootobj = cref.objectroot
        self._workspace_root = rootobj
        
        self._load_links(rootobj)
        
        
        self._detached_head = detached
        if detached:
            self._current_branch = self.create_wrapped_object(mutable_pb2.Branch, addtoworkspace=False)
            self._current_branch.set_link_by_name('commitref', cref)
            self._current_branch.branchkey = 'detached head'
            
            rootobj._set_structure_read_only()
            
        else:
            self._current_branch = branch
        return rootobj
        
    def reset(self):
        
        if self.status != self.MODIFIED:
            # What about not initialized
            return
        
        cref = self._current_branch.commitref
        
        # Do some clean up!
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
            
        if self.status == self.MODIFIED:
            structure={}
            self._workspace_root._recurse_commit(structure)
                                
            cref = self._create_commit_ref(comment=comment)
                
            # Add the CRef to the hashed elements
            cref._recurse_commit(structure)
            
            # set the cref to be readonly
            cref.readonly = True
            
            # Add the cref to the active commit objects - for convienance
            self._commit_index[cref.myid] = cref

            # update the hashed elements
            self._hashed_elements.update(structure)
                            
        elif self.status == self.UPTODATE:
            pass
        else:
            raise Exception, 'Repository in invalid state to commit'
        
        # Like git, return the commit id
        branch = self._current_branch
        return branch.get_link('commitref').key
            
            
    def _create_commit_ref(self, comment='', date=None):
        """
        @brief internal method to create commit references
        @param comment a string that describes this commit
        @param date the date to associate with this commit. If not given then 
        the current time is used.
        @retval a string which is the commit reference
        """
        # Now add a Commit Ref     
        cref = self.create_wrapped_object(mutable_pb2.CommitRef, addtoworkspace=False)
        
        if not date:
            date = pu.currenttime()
            
        cref.date = date
        
        branch = self._current_branch

        # If this is the first commit to a new repository the current branch is a dummy
        # If it is initialized it is real and we need to link to it!
        if branch.IsInitialized():
            
            # This branch is real - add it to our ancestors
            pref = cref.parentrefs.add()
            parent = branch.commitref # get the parent commit ref
            pref.set_link_by_name('commitref',parent)
            pref.relationship = pref.Parent
        
        # For each branch that we merged from - add a  reference
        for mrgd in self._merged_from:
            pref = cref.ancestors.add()
            merged_commit = mrgd.commitref # Get the commit ref of the merged item
            pref.set_link_by_name('commitref',merged_commit)
            pref.relationship = pref.MergedFrom
            
        cref.comment = comment
        cref.set_link_by_name('objectroot', self._workspace_root)            
        
        # Update the cref in the branch
        branch.set_link_by_name('commitref',cref)
        
        return cref
    
            
        
    def merge(self, branch=None, commit_id = None, older_than=None):
        """
        merge the named branch in to the current branch
        """
        
        
    @property
    def status(self):
        """
        Check the status of the current workspace - return a status
          up to date
          changed
        """
        
        if self._workspace_root:
            if self._workspace_root.modified:
                return self.MODIFIED
            else:
                return self.UPTODATE
        else:
            return self.NOTINITIALIZED
        
        
    def branch(self, nickname=None):
        """
        @brief Create a new branch from the current commit and switch the workspace to the new branch.
        """
        ## Need to check and then clear the workspace???
        #if not self.status == self.UPTODATE:
        #    raise Exception, 'Can not create new branch while the workspace is dirty'
        
        brnch = self._dotgit.branches.add()    
        brnch.branchkey = pu.create_guid()
        
        
        
        if nickname:
            self.branchnicknames[nickname]=brnch.branchkey

        if self._current_branch:
            # Get the linked commit
            cref = self._current_branch.commitref
            
            # Set the new branch to point at the commit
            brnch.set_link_by_name('commitref',cref)
            
            
            # Making a new branch re-attaches to a head!
            if self._detached_head:
                self._workspace_root._set_structure_read_write()
                self._detached_head = False
                
        self._current_branch = brnch
        
        
    
    def stash(self, name):
        """
        Stash the current workspace for later reference
        """
        
    def create_wrapped_object(self, rootclass, obj_id=None, addtoworkspace=True):        
        
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
        obj._read_only = False
        obj._myid = obj_id
        obj._modified = True     

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
                
        if link.GPBType != self.LinkClassType:
            raise Exception, 'Illegal argument type in get_linked_object.'
                
                
        if not link.HasField('key'):
            return None
                
        if self._workspace.has_key(link.key):
            return self._workspace.get(link.key)

        elif self._commit_index.has_key(link.key):
            return self._commit_index.get(link.key)

        elif self._hashed_elements.has_key(link.key):
            
            element = self._hashed_elements.get(link.key)
            
            
            if not link.type.package == element.type.package and \
                    link.type.cls == element.type.cls:
                raise Exception, 'The link type does not match the element type!'
            
            obj = self._load_element(element)
            
            if obj.GPBType == self.CommitClassType:
                self._commit_index[obj.myid]=obj
                obj.readonly = True
            else:
                self._workspace[obj.myid]=obj
                obj.readonly = self._detached_head
            return obj
            
        else:
            raise Exception, 'Object not in workbench! You must pull the leaf elements!'
            #return self._workbench.fetch_linked_objects(link)
            
    def _load_links(self, obj, loadleaf=False):
        """
        Load the child objects into the work space
        """        
        if loadleaf:
            
            for link in obj._child_links:
                child = self.get_linked_object(link)  
                self._load_links(child, loadleaf=loadleaf)
        else:
            for link in obj._child_links:
                
                if not link.isleaf:
                    child = self.get_linked_object(link)      
                    self._load_links(child, loadleaf=loadleaf)
        
        
            
    def _load_element(self, element):
        
        #log.debug('_load_element' + str(element))
        
        mysha1 = gpb_wrapper.sha1hex(element.value)
        assert element.key == mysha1, \
            'The sha1 key does not match the value. The data is corrupted! \n' +\
            'Element key %s, Calculated key %s' % (element.key, mysha1)
        
        cls = self._load_class_from_type(element.type)
                                
        # Do not automatically load it into a particular space...
        obj = self.create_wrapped_object(cls, obj_id=element.key, addtoworkspace=False)
            
        obj.ParseFromString(element.value)
        
        # If it is not a leaf element - find its child links
        if not element.isleaf:
            obj._find_child_links()

        obj.modified = False
        
        # Make a note in the element of the child links as well!
        for child in obj._child_links:
            element._child_links.add(child.key)
        
        return obj
        
    def _load_class_from_type(self,ltype):
    
        module = str(ltype.protofile) + '_pb2'
                
        cls_name = str(ltype.cls)
        
        package = str(ltype.package)
        
        log.debug('Loading Class from Type: Package - %s, Module - %s, Class - %s'\
            % (package, module, cls_name))
        
        path = package + '.' + module
        __import__(path)
        
        mod = sys.modules[package+'.'+module]
        
        cls = getattr(mod, cls_name)
                
        return cls
        
        
    def _set_type_from_obj(self, ltype, wrapped_obj):
        """
        This method is a bit of a mess - do we really need it?
        
        It opperates directly on unwrapped GPB objects
        """
            
        obj = wrapped_obj
        if isinstance(obj, gpb_wrapper.Wrapper):
            obj = obj.GPBMessage
            
        gpbtype = gpb_wrapper.set_type_from_obj(obj)
        
        thetype = ltype
        if isinstance(thetype, gpb_wrapper.Wrapper):
            thetype=ltype.GPBMessage
            
        thetype.CopyFrom(gpbtype)
        
        
        
    def set_linked_object(self,field, value):        
        # If it is a link - set a link to the value in the wrapper
        if field.GPBType == field.LinkClassType:
            
            #@Todo Change assertions to Exceptions?
            
            assert value.isroot == True, \
                'You can not set a link equal to part of a gpb composite!'
            
            assert not field.inparents(value), \
                'You can not create a recursive structure - this value is also a parent of the link you are setting.'
            
            
            #Make sure the link is in the objects set of child links
            field._child_links.add(field) # Adds to the fields root wrapper!
            value._parent_links.add(field) 
            
            # If the link is currently set
            if field.key:
                                
                if field.key == value.myid:
                    # Setting it again is a pass...
                    return
                
                
                old_obj = self._workspace.get(field.key,None)
                if old_obj:
                    plinks = old_obj._parent_links
                    plinks.remove(field.key)
                    # If there are no parents left for the object delete it
                    if len(plinks)==0:
                        del self._workspace[field.key]
                    
                
                # Modify the existing link
                field.key = value.myid
                
                # Set the new type
                tp = field.type
                self._set_type_from_obj(tp, value)
                    
            else:
                
                # Set the id of the linked wrapper
                field.key = value.myid
                
                # Set the type
                tp = field.type
                self._set_type_from_obj(tp, value)
                
        else:
            
            raise Exception, 'Can not set a composite field'
            #Over ride Protobufs - I want to be able to set a message directly
        #    field.CopyFrom(value)
        
            