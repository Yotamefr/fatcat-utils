from fatcat_utils.logger import generate_logger


def test_logger():
    logger = generate_logger("TestLogger")
    logger.info("test")

    logger = generate_logger("TestingLogger")
    try:
        raise Exception("Snappy snap")
    except Exception as e:
        logger.error("Oh snap", exc_info=e)
