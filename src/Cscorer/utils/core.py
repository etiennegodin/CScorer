




async def _ask_yes_no(msg:str):
    values = ['y','n']
    correct = False
    while (not correct):
        answer = input(msg)
        if answer.lower() in values:
            correct = True
    
    if answer == 'y':
        return True
    else:
        return False