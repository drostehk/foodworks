from termcolor import colored, cprint

def print_error(error, expected, value=None):
    print("\n{: ^80}\n".format(" << {} : {} {} >> ".format("ERROR", error, "*" + value + "*" if value else "")).upper())
    print("\n{: ^80}\n".format(" Valid Values : {}".format(", ".join(expected))))


def print_warning(warning, msg=None):
    print("\n{: ^80}\n".format(" << {} : {} >> ".format("WARNING", warning.upper())))
    if msg:
        print("\n{: ^80}\n".format(" RESULT : {}".format(msg)))

def print_header(header, color='red'):
    msg = colored('{:^80}'.format(header.upper()), color, attrs=['reverse','bold'])
    print('\n')
    print(msg)
    print('\n')

def print_sub_header(header, color='red'):
    msg = colored(' {:>20}'.format(header.upper()+"  "), color, attrs=['reverse','bold'])
    print('\n')
    print(msg)
    print('\n')

def print_action(action, item, number):
    print('{:10s} {:3d}  {:7.2f}'.format(action, item, number))

def print_status(status, item, number):
    if status.lower() in ['failed','skipped']:
        status = colored(status.upper(), 'red')
    elif status.lower() == 'exporting':
        status = colored(status.upper(), 'magenta')
    else:
        status = colored(status.upper(), 'blue')

    msg = '{:^20} {:^55} {:^5s}'.format(status, item, number)
    print('{:^80}'.format(msg))
