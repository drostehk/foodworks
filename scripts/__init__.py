def print_error(error, expected, value=None):
    print("\n{: ^80}\n".format(" << {} : {} {} >> ".format("ERROR", error, "*" + value + "*" if value else "")).upper())
    print("\n{: ^80}\n".format(" Valid Values : {}".format(", ".join(expected))))


def print_warning(warning, msg=None):
    print("\n{: ^80}\n".format(" << {} : {} >> ".format("WARNING", warning.upper())))
    if msg:
        print("\n{: ^80}\n".format(" RESULT : {}".format(msg)))
