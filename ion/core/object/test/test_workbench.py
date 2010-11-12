#!/usr/bin/env python
"""
@Brief Test implementation of the workbench class

@file ion/core/object
@author David Stuebe
@test The object managment WorkBench class
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from uuid import uuid4

from twisted.trial import unittest
#from twisted.internet import defer

from ion.test.iontest import IonTestCase

from net.ooici.play import addressbook_pb2

from ion.core.object import gpb_wrapper
from ion.core.object import repository
from ion.core.object import workbench


class WorkBenchTest(unittest.TestCase):
        
    def setUp(self):
        wb = workbench.WorkBench('No Process Test')
        self.wb = wb
        
        repo, ab = self.wb.init_repository(addressbook_pb2.AddressLink)

                        
        p = repo.create_wrapped_object(addressbook_pb2.Person)
        p.name='David'
        p.id = 5
        p.email = 'd@s.com'
        ph = p.phone.add()
        ph.type = p.WORK
        ph.number = '123 456 7890'
        
        ab.owner = p
            
        ab.person.add()
        ab.person[0] = p
        
        ab.person.add()
        p = repo.create_wrapped_object(addressbook_pb2.Person)
        p.name='John'
        p.id = 78
        p.email = 'J@s.com'
        ph = p.phone.add()
        ph.type = p.WORK
        ph.number = '111 222 3333'
        
        ab.person[1] = p
        
        self.ab = ab
        self.repo = repo
        
        
            
    def test_simple_commit(self):
        
        cref = self.repo.commit(comment='testing commit')
        print 'Commited',cref
        
        
    
    def test_pack_mutable(self):
        serialized = self.wb.pack_structure(self.repo._dotgit)
        
        
    def test_pack_root_eq_unpack(self):
        
        serialized = self.wb.pack_structure(self.ab)
            
        res = self.wb.unpack_structure(serialized)
        
        self.assertEqual(res,self.ab)
        
        
    def test_pack_mutable_eq_unpack(self):
            
        serialized = self.wb.pack_structure(self.repo._dotgit)
            
        repo = self.wb.unpack_structure(serialized)
        
        self.assertEqual(repo._dotgit, self.repo._dotgit)
        
        ab=repo.checkout(branch='master')
        
        self.assertEqual(ab, self.ab)
            
            
        
    def test_pack_repository_commits(self):
        
        self.repo.commit('testing repository packing')
        
        serialized = self.wb.pack_repository_commits(self.repo)
        
        repo = self.wb.unpack_structure(serialized)
        
        self.assertEqual(repo._dotgit, self.repo._dotgit)
        
        commit = repo._dotgit.branches[0].commitref
        
        #Check that the commit came through
        self.assertEqual(commit, self.repo._current_branch.commitref)
        
        
    def test_init_repo(self):
            
        # Try it with no arguments
        repo, rootobj = self.wb.init_repository()
            
        rkey = repo.repository_key
        self.assertEqual(repo, self.wb.get_repository(rkey))
        self.assertEqual(rootobj, None)
            
            
        # Try it with a root object this time
        repo, rootobj = self.wb.init_repository(rootclass=addressbook_pb2.AddressBook)
            
        rkey = repo.repository_key
        self.assertEqual(repo, self.wb.get_repository(rkey))
        self.assertIsInstance(rootobj, gpb_wrapper.Wrapper)
            
        # Try it with a nickname for the repository
        repo, rootobj = self.wb.init_repository(rootclass=addressbook_pb2.AddressBook, nickname='David')
            
        self.assertEqual(repo, self.wb.get_repository('David'))
        self.assertIsInstance(rootobj, gpb_wrapper.Wrapper)
        
        
        
class WorkBenchMergeTest(unittest.TestCase):
        
    def test_merge(self):
        wb1 = workbench.WorkBench('No Process Test')
        
        repo1, ab = wb1.init_repository(addressbook_pb2.AddressBook)
        
        commit_ref1 = repo1.commit()
        p = ab.person.add()
        p.id = 1
        p.name = 'Uma'
            
        commit_ref2 = repo1.commit()
            
        p.name = 'alpha'
        commit_ref3 = repo1.commit()
            
        ab = repo1.checkout(commit_id=commit_ref1)
        self.assertEqual(len(ab.person),0)
            
        ab = repo1.checkout(commit_id=commit_ref2)
        self.assertEqual(ab.person[0].id,1)
        self.assertEqual(ab.person[0].name,'Uma')
        
        ab = repo1.checkout(branch='master')
        self.assertEqual(ab.person[0].id,1)
        self.assertEqual(ab.person[0].name,'alpha')

        # Serialize it
        serialized = wb1.pack_repository_commits(repo1)
        
        # Create a new, separate work bench and read it!
        wb2 = workbench.WorkBench('No Process Test')
        repo2 = wb2.unpack_structure(serialized)
        
        print 'BRANCHES', repo2.branches[0]
        
        # Can't pull the objects from the other repository! Because it is not a process!
        ab = repo2.checkout(branch='master')
        
        print 'AB',ab
        
        ab = repo2.checkout(commit_id=commit_ref1)
        self.assertEqual(len(ab.person),0)
            
        ab = repo2.checkout(commit_id=commit_ref2)
        self.assertEqual(ab.person[0].id,1)
        self.assertEqual(ab.person[0].name,'Uma')
        
        ab = repo2.checkout(commit_id=commit_ref3)
        self.assertEqual(ab.person[0].id,1)
        self.assertEqual(ab.person[0].name,'alpha')
        
        # Back to WB1 - add some more commits!
        
        ab = repo1.checkout(commit_id=commit_ref3)
        
        
        
        
        
                        
        
        