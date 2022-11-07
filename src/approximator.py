import sympy
from sympy.utilities.lambdify import implemented_function, lambdify
import numpy as np
import logging

LOGGER = logging.getLogger(__name__)

a, b, c, d, x = sympy.symbols("a, b, c, d, x")

## sum of two quadratic functions - 2 leakages
f = implemented_function('f', lambda x: -a**2 + b - c**2 + d + (a*x)/20 + (c*x)/20 - x**2/800) 
f_sy = lambdify(x, f(x)) ## parse for sympy usability

def solve_double(noise_values_list, list_of_sections):
    #[[x, y], [x, y], [x, y]]
    res = {}
    nvl = noise_values_list
    eq = [f_sy(nvl[0][0])-nvl[0][1], f_sy(nvl[1][0])-nvl[1][1], f_sy(nvl[2][0])-nvl[2][1], f_sy(nvl[3][0])-nvl[3][1]]

    for sec in list_of_sections:
        try:
            result = sympy.nonlinsolve(eq+[c-sec], [a, c, b, d])
        

            result = result.args[0][0]
            print("\n", result)
            result_a = result.subs(b, 20).subs(d, 20).subs(c, sec)
            if type(result_a) == sympy.core.numbers.Float:
                res[sec] = result_a
            else:
                LOGGER.warning("Result is a complex number.")           
        except:
            LOGGER.error("Did not converge.")


    return res