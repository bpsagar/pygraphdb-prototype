__author__ = 'Sagar'
from pyparsing import Literal, Word, alphanums, Forward, ZeroOrMore, Group

class ExpressionTree(object):
    def __init__(self, expression):
        self._root_node = None
        self._expression = expression

    def get_root_node(self):
        return self._root_node

    def build_tree(self, postfix):
        stack = []
        ops = ['==', '>', '<', '>=', '<=']
        cds = ['&&', '||']
        for i in postfix:
            if isinstance(i, str):
                if i not in ops and i not in cds:
                    stack.append(ExpressionTreeNode(i))
                else:
                    right = stack.pop()
                    left = stack.pop()
                    stack.append(ExpressionTreeNode(i, left, right))
            else:
                stack.append(self.build_tree(i))
        return stack.pop()

    def infix_to_postfix(self, infix):
        postfix = []
        stack = [' ']
        ops = ['==', '>', '<', '>=', '<=']
        cds = ['&&', '||']

        for token in infix:
            if isinstance(token, str):
                if token in ops or token in cds:
                    if self.priority(token) > self.priority(stack[-1]):
                        stack.append(token)
                    else:
                        while self.priority(token) <= self.priority(stack[-1]):
                            postfix.append(stack.pop())
                        stack.append(token)
                else:
                    postfix.append(token)
            else:
                postfix.append(self.infix_to_postfix(token))
        while len(stack) > 1:
            postfix.append(stack.pop())
        return postfix

    def priority(self, c):
        ops = ['==', '>', '<', '>=', '<=']
        cds = ['&&', '||']
        if c in ops:
            return 2
        if c in cds:
            return 1
        return -1

    def initialise(self):
        postfix = self.infix_to_postfix(self._expression)
        self._root_node = self.build_tree(postfix)

    def display(self):
        stack = []
        stack.append(self._root_node)
        while len(stack) > 0 :
            node = stack.pop()
            print(node.get_value())
            if node.get_left_node() is not None:
                stack.append(node.get_left_node())
            if node.get_right_node() is not None:
                stack.append(node.get_right_node())

class ExpressionTreeNode(object):
    def __init__(self, value, left=None, right=None):
        super(ExpressionTreeNode, self).__init__()
        self._value = value
        self._left_node = left
        self._right_node = right

    def get_value(self):
        return self._value

    def get_left_node(self):
        return self._left_node

    def get_right_node(self):
        return self._right_node