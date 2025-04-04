"""
    这个包提供了一个全局随机生成器，可以用determinsim运行实验。
    所有使用get_rand_gen的代码都将使用相同的随机生成器，如果先前set_globral_random_ge的一个参数不是None。
    为了重复实验，在实验前使用set_global_random_gen，使用相同的“seed”值。
    该文件提供了一个全局随机生成器，用于确保实验的可重复性。通过设置全局随机生成器的种子，可以在不同的运行中获得相同的结果。
"""

import random

global_random_gen = None

def set_global_random_gen(seed=None,random_gen=None):
    """使用整个包获得随机生成器的所有代码。将其设置为相同的种子初始化随机对象。
    Args:
        - seed: 如果设置为_hashhable_ obj，则以seed作为种子创建一个新的随机生成器。
        - random_gen: 要设置为全局的随机对象。如果设置了seed，则忽略此参数。
    """
    global global_random_gen
    if seed is not None:
        global_random_gen = random.Random()
        global_random_gen.seed(seed)
    else:
        global_random_gen = random_gen
        
def get_random_gen(seed=None):
    """如果设置了全局随机生成器，则返回全局随机生成器，否则创建新的随机生成器。如果种子已经播下，它就会被用于这个目的。"""
    global global_random_gen
    if global_random_gen is not None:
        return  global_random_gen
    r = random.Random()
    r.seed(a=seed)
    return r