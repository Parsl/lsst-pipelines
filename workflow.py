
# this attempts to run the tutorial at https://pipelines.lsst.io/getting-started/data-setup.html
# assume various setup commands have been run
# and that git-lfs has been installed

import logging
import parsl
import parsl.config
import parsl.utils
import parsl.executors

from parsl.monitoring.monitoring import MonitoringHub
from parsl.addresses import address_by_hostname

logger = logging.getLogger(__name__)

parsl.set_stream_logger()
parsl.set_stream_logger(__name__)

logger.info("Logging should be initialised now")

config = parsl.config.Config(
    executors=[parsl.executors.ThreadPoolExecutor(label="management", max_threads=20),
               parsl.executors.ThreadPoolExecutor(label="heavy", max_threads=3),
              ],

    # monitoring config from https://parsl.readthedocs.io/en/latest/userguide/monitoring.html
    # modified to add hub_port - see https://github.com/Parsl/parsl/issues/1010
    monitoring=MonitoringHub(
        hub_address=address_by_hostname(), logging_level=logging.INFO, resource_monitoring_interval=10,
        hub_port=30733
    )

   )

config.checkpoint_mode = 'task_exit'

global_repo="DATA_GR"

logger.info("Getting checkpoint files")
config.checkpoint_files = parsl.utils.get_all_checkpoints()
logger.info("Checkpoint files: {}".format(config.checkpoint_files))

logger.info("Defining tutorial import subroutine")

# the data files created in this app need to be persistent
# the stuff under DATA/ is a permanent data store
# and the stuff under ci_hsc is symlinked in - maybe it should
# be hardlinked or copied? so that the ci_hsc stuff can be
# a transient working directory?

@parsl.bash_app(cache=True, executors=["heavy"])
def create_empty_repo(repo: str):
    return "rm -rf {r} && mkdir {r} && echo lsst.obs.hsc.HscMapper > {r}/_mapper".format(r=repo)

@parsl.bash_app(cache=True, executors=["heavy"])
def install_transmission_curves(repo: str):
    return "installTransmissionCurves.py {r}".format(r=repo)

# the tutorial says should use ingestCalibs but they take a short cut
# and do it the "wrong" way using ln - could replace that?
@parsl.bash_app(cache=True, executors=["heavy"])
def import_ci_hsc(repo: str, stdout="ingest.default.stdout", stderr="ingest.default.stderr"):
    return "rm -rf ci_hsc && git clone https://github.com/lsst/ci_hsc && setup -j -r ci_hsc && ingestImages.py {r} $CI_HSC_DIR/raw/*.fits --mode=link && ln -s $CI_HSC_DIR/CALIB/ {r}/CALIB && mkdir -p {r}/ref_cats && ln -s $CI_HSC_DIR/ps1_pv3_3pi_20170110 {r}/ref_cats/ps1_pv3_3pi_20170110 ".format(r=repo)

# this assumes that we're running in the same
# python process - using thread local executor -
# so that it will have access to globals.
def tutorial_1_import():
    logger.info("starting data import")

    empty_repo_future = create_empty_repo(global_repo)
    empty_repo_future.result()

    transmission_curve_future = install_transmission_curves(global_repo)
    import_future = import_ci_hsc(global_repo)

    transmission_curve_future.result()
    import_future.result()

    logger.info("ended data import")


# pccd_show and pccd_process could be refactored,
# with a bool parameter?
@parsl.bash_app(cache=True, executors=["heavy"])
def pccd_show(repo: str, stdout="pccd_show.default.stdout"):
    return "processCcd.py {r} --rerun rr-processccd-show --id --show data".format(r=repo)

@parsl.bash_app(cache=True, executors=["heavy"])
def pccd_process(repo: str, stdout="pccd_process.default.stdout"):
    return "processCcd.py {r} --rerun rr-processccd-show --id".format(r=repo)

def tutorial_2_show_data():
    logger.info("running some processCcd task")
    # These two could run in parallel as a demo of running stuff
    # in parallel

    # ideally they'd not take global_repo as an input, but some descriptor
    # that would come back from the tutorial_1_import stage...
    # and then the earlier tasks would not have to do a result wait at all
    # but instead the descriptor would live inside a future and provide
    # the dependencies there.

    pccd_show_future = pccd_show(global_repo)
    pccd_process_future = pccd_process(global_repo)

    pccd_show_future.result()
    pccd_process_future.result()

    logger.info("finished processCcd tasks")

@parsl.bash_app(cache=True, executors=["heavy"])
def makeDiscreteSkyMap(repo: str):
    return "makeDiscreteSkyMap.py {r} --id --rerun rr-processccd-show:wfdev2 --config skyMap.projection=TAN".format(r=repo)


@parsl.bash_app(cache=True, executors=["heavy"])
def makeCoaddTempExp(repo: str, filter: str):
    return "makeCoaddTempExp.py {r} --rerun wfdev2 --selectId filter={f} --id filter={f} tract=0 patch=0,0^0,1^0,2^1,0^1,1^1,2^2,0^2,1^2,2 --config doApplyUberCal=False doApplySkyCorr=False".format(r=repo, f=filter)

@parsl.bash_app(cache=True, executors=["heavy"])
def assembleCoadd(repo: str, filter: str): 
    return "assembleCoadd.py {r} --rerun wfdev2 --selectId filter={f} --id filter={f} tract=0 patch=0,0^0,1^0,2^1,0^1,1^1,2^2,0^2,1^2,2".format(r=repo, f=filter)

@parsl.python_app(cache=True, executors=["management"])
def tutorial_4_apps(global_repo: str, filter: str):
    f1 = makeCoaddTempExp(global_repo, filter)
    f1.result()
    f2 = assembleCoadd(global_repo, filter)
    f2.result()

def tutorial_4_coadd():
    logger.info("assembling processed CCD images into sky map")

    f1 = makeDiscreteSkyMap(global_repo)
    f1.result()

    # Assumption: I think HSC-R and HSC-I processing is entirely separate so the two
    # pieces can run in parallel?
    futures = []
    for filter in ["HSC-R", "HSC-I"]:
        logger.info("launching apps for filter {}".format(filter))
        futures.append(tutorial_4_apps(global_repo, filter))

    for future in futures:
        logger.info("waiting for a future")
        future.result()
        

    # TODO: now wait for these to finish in appropriate pattern...
    logger.info("finished assembling processed CCD images into sky map")

parsl.load(config)

tutorial_1_import()

tutorial_2_show_data()

tutorial_4_coadd()


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
