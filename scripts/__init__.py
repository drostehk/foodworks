from termcolor import colored, cprint

def print_error(error, expected, value=None):
    print("\n{: ^80}\n".format(" << {} : {} {} >> ".format("ERROR", error, "*" + value + "*" if value else "")).upper())
    print("\n{: ^80}\n".format(" Valid Values : {}".format(", ".join(expected))))

def print_data_error(error, msg, action='Make a Coffee',exit=False):
    print("\n{: ^80}\n".format(" << {} : {} >> ".format("ERROR", error, "*" + msg.split(':')[0] + "*" if ':' in msg else "DATA ERROR")).upper())
    if ':' in msg:
        print("\n{: ^80}\n".format(msg.split(':')[1]))
    print("\n{: ^80}\n".format(" Suggested Action : {}".format(action)))
    if exit:
        raise SystemExit

def print_warning(warning, msg=None):
    print("\n{: ^80}\n".format(" << {} : {} >> ".format("WARNING", warning.upper())))
    if msg:
        print("\n{: ^80}\n".format(" RESULT : {}".format(msg)))

def print_header(header, color='red'):
    msg = colored('{:^80}'.format(header.upper()), color, attrs=['reverse','bold'])
    print('\n')
    print(msg)
    print('')

def print_sub_header(header, color='red',center=False):
    print(' ')
    if center:
        msg = colored(' {} '.format(header.upper()), color, attrs=['reverse', 'bold'])
        print("{:^96}".format(msg))
    else:
        msg = colored(' {:>20}'.format(header.upper()+"  "), color, attrs=['reverse','bold'])
        print(msg)
    print(' ')

def print_action(action, item, number):
    print('{:10s} {:3d}  {:7.2f}'.format(action, item, number))

def print_status(status, item, number):
    if status.lower() in ['failed','skipped']:
        status = colored(status.upper(), 'red')
    elif status.lower() == 'exporting':
        status = colored(status.upper(), 'magenta')
    else:
        status = colored(status.upper(), 'blue')

    msg = '{:^20} {:^53} {:>.7s}'.format(status, item, number.upper())
    print('{:<80}'.format(msg))
