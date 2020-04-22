#!/usr/bin/env python3

"""Binary Tree Reversals
Implement in-order, pre-order, and post-order traversals of a binary tree.

Given a binary rooted tree, build and output its in-order, pre-order, and
post-order traversals.

"""

import sys, threading
sys.setrecursionlimit(10**6) # max depth of recursion
threading.stack_size(2**27)  # new thread will get stack of such size

class TreeOrders:

    def read(self):

    self.n = int(sys.stdin.readline())
    self.key = [0 for i in range(self.n)]
    self.left = [0 for i in range(self.n)]
    self.right = [0 for i in range(self.n)]
    for i in range(self.n):
      [a, b, c] = map(int, sys.stdin.readline().split())
      self.key[i] = a
      self.left[i] = b
      self.right[i] = c

    def inOrder(self):
    """ docstring
    recursively do an inorder traversal on the left subtree, visit the root
    node, and finally do a recursive inorder traversal of the right subtree
    """
    self.result = []
    self.inOrderWalk(0)
    return self.result

    def inOrderWalk(self, index):
        if (self.left[index] != -1):
            self.inOrderWalk(self.left[index])
        self.result.append(self.key[index])
        if (self.right[index] != -1):
            self.inOrderWalk(self.right[index])

    def preOrder(self):
    """doctring
    visit root node first, then recursively do a preorder traversal of the left
    subtree, followed by a recursive preorder traversal of the right subtree
    """
    self.result = []
    self.preOrderWalk(0)
    return self.result

    def preOrderWalk(self, index):
        self.result.append(self.key[index])
        if (self.left[index] != -1):
            self.preOrderWalk(self.left[index])
        if (self.right[index] != -1):
            self.preOrderWalk(self.right[index])

    def postOrder(self):
    """docstring
    recursively do a postorder traversal of the left subtree and the right
    subtree followed by a visit to the root node
    """
    self.result = []
    self.postOrderWalk(0)
    return self.result

    def postOrderWalk(self, index):
        if (self.left[index] != -1):
            self.postOrderWalk(self.left[index])
        if (self.right[index] != -1):
            self.postOrderWalk(self.right[index])
        self.result.append(self.key[index])

def main():
	tree = TreeOrders()
	tree.read()
	print(" ".join(str(x) for x in tree.inOrder()))
	print(" ".join(str(x) for x in tree.preOrder()))
	print(" ".join(str(x) for x in tree.postOrder()))

threading.Thread(target=main).start()
