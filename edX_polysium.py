import math

def polysium(n, s):
    area = (0.25 * n * (s**2)) / math.tan(math.pi / n)
    perimeter = n * s

    return round(area + perimeter**2, 4)
