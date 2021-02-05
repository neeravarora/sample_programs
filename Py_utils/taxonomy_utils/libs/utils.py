import os


def dict_append(dict1, dict2):
    dict1.update(dict2)
    return dict1


def transpose_dict(dict_arg):
    res = defaultdict(set)
    [res[t].add(k) for k, v in dict_arg.items() for t in v]
    return dict(res)

def path_resolve(parent_path, relative_path):
    path_tk = parent_path.split('//')
    if len(path_tk) == 2: 
        abc = os.path.join(path_tk[1], relative_path)
    else:
        abc = os.path.join(path_tk[0], relative_path)
    folder_token_stk = abc.split('/')

    cnt = 0
    for idx, tk in enumerate(folder_token_stk):
        if tk == '.':
            pass
        elif tk == '..':
            cnt = cnt-1
        else:
            folder_token_stk[cnt] = tk
            cnt = cnt+1

    if len(path_tk) == 2:
        return '{}//{}'.format(path_tk[0], '/'.join(folder_token_stk[:cnt]))
    else:
        return '/{}'.format('/'.join(folder_token_stk[:cnt]))