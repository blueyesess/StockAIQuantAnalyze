# ½ø¶ÈÌõ
def bar(now, total, prefix='progress:', last_process=0):
    ratio = now / total * 100
    print('\r' + prefix + f'[ {ratio:.2f}%] ', end='')
    return ratio > last_process
