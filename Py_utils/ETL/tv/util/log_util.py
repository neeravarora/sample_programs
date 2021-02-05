import logging, sys

def print_log_banner(msg):
    # FORMAT = "%(message)s"
    # formatter = logging.Formatter(FORMAT)
    # stdout_handler = logging.StreamHandler(sys.stdout)
    # stdout_handler.setFormatter(formatter)
    # logger = logging.getLogger(__name__)
    # for hdlr in logger.handlers:  # remove all old handlers
    #     logger.removeHandler(hdlr)
    # logger.addHandler(stdout_handler)


    # for handler in logger.handlers:
    #     if existing_formatter:
    #         existing_formatter = handler.formatter
    #     handler.setFormatter(formatter)
    # logger.info("\n\n")
    # logger.info("    ********************************************************************************")
    # logger.info("    **                                                                            **")
    # logger.info("    **                                                                            **")
    # logger.info("    **         "+msg)
    # logger.info("    **                                                                            **")
    # logger.info("    **                                                                            **")
    # logger.info("    ********************************************************************************")
    # logger.info("\n\n")
    # for handler in logger.handlers:
    #     handler.setFormatter(existing_formatter)
    space_len = 4
    stars_len = 70
    max_msg_len = 30
    print("\n\n")
    print("    ********************************************************************************")
    print("    ***                                                                          ***")
    print("    ***                                                                          ***")
    print("    ***         "+msg)
    print("    ***                                                                          ***")
    print("    ***                                                                          ***")
    print("    ********************************************************************************")
    print("\n\n")