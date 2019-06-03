
# this attempts to run the tutorial at https://pipelines.lsst.io/getting-started/data-setup.html
# assume various setup commands have been run
# and that git-lfs has been installed

import logging
import parsl
import parsl.utils

from parsl.configs.local_threads import config
config.checkpoint_mode = 'task_exit'

logger = logging.getLogger(__name__)

parsl.set_stream_logger()
parsl.set_stream_logger(__name__)

logger.info("Logging should be initialised now")

logger.info("Getting checkpoint files")
config.checkpoint_files = parsl.utils.get_all_checkpoints()
logger.info("Checkpoint files: {}".format(config.checkpoint_files))

logger.info("Defining tutorial import subroutine")

# the data files created in this app need to be persistent
# the stuff under DATA/ is a permanent data store
# and the stuff under ci_hsc is symlinked in - maybe it should
# be hardlinked or copied? so that the ci_hsc stuff can be
# a transient working directory?
@parsl.bash_app(cache=True)
def import_ci_hsc():
    return "rm -rf DATA && rm -rf ci_hsc && git clone https://github.com/lsst/ci_hsc && setup -j -r ci_hsc && mkdir DATA && echo lsst.obs.hsc.HscMapper > DATA/_mapper && ingestImages.py DATA $CI_HSC_DIR/raw/*.fits --mode=link && installTransmissionCurves.py DATA && ln -s $CI_HSC_DIR/ci_hsc/CALIB/ DATA/CALIB && mkdir -p DATA/ref_cats && ln -s $CI_HSC_DIR/ci_hsc/ps1_pv3_3pi_20170110 DATA/ref_cats/ps1_pv3_3pi_20170110 "

# this assumes that we're running in the same
# python process - using thread local executor -
# so that it will have access to globals.
def tutorial_1_import():
    logger.info("starting data import")

    import_future = import_ci_hsc()
    import_future.result()
 

    logger.info("ended data import")


@parsl.bash_app(cache=True)
def pccd_show(stdout="pccd_show.default.stdout"):
    return "processCcd.py DATA --rerun rr-processccd-show --id --show data"

def tutorial_2_show_data():
    logger.info("asking processCcd.py to show data")
    pccd_future = pccd_show()
    pccd_future.result()
    logger.info("finished asking processCcd.py to show data")


parsl.load(config)

tutorial_1_import()

tutorial_2_show_data()

# this will in passing create a rerun directory parented to DATA
# but won't actually put anything in it apart from the parenting
# metadata. 
# processCcd.py DATA --rerun rr-processccd-show --id --show data


# 1. subroutine for setting things up
# get some sample data

# git clone https://github.com/lsst/ci_hsc

# installTransmissionCurves.py DATA

# DATA is a directory name... should it be parameterised and what can it be?


# this is some kind of sly import...
# ln -s $CI_HSC_DIR/CALIB/ DATA/CALIB
# mkdir -p DATA/ref_cats
# ln -s $CI_HSC_DIR/ps1_pv3_3pi_20170110 DATA/ref_cats/ps1_pv3_3pi_20170110


# 2. process ccd data a bit

# processCcd.py DATA --rerun processCcdOutputs --id --show data

# which will give lots of human readable text output - direct that human
# readable text output on stdout to a text file that will be part of the
# final output
# and perhaps read it in or display it or display the pathname or
# something?

logger.info("reached end of workflow script")
