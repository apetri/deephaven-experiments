"""
Define utils that are used in DQL query scope
"""

import numpy as np

def rndString(*choices) -> str:
    return np.random.choice(choices)

def rndInt(*choices) -> np.int32:
    return np.random.choice(choices)

def rndUnif(*params) -> float:
    return np.random.uniform(params[0],params[1])