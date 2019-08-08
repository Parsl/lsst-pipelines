
# this attempts to run part of the tutorial at
#   https://pipelines.lsst.io/getting-started/data-setup.html

import logging
import random

import parsl

logger = logging.getLogger(__name__)

parsl.set_stream_logger()
parsl.set_stream_logger(__name__)

logger.info("Importing parsl modules")

import parsl.config
import parsl.utils
import parsl.executors

from parsl.monitoring.monitoring import MonitoringHub
from parsl.addresses import address_by_hostname

logger.info("Importing lsst modules")

logger.info("Done with imports")

# TODO: proper boolean switch here to switch between checkpointing and
# monitoring, as they do not work together at the moment.
#  - see https://github.com/Parsl/parsl/issues/1014


config = parsl.config.Config(
    executors=[parsl.executors.ThreadPoolExecutor(label="management",
                                                  max_threads=20),
               parsl.executors.ThreadPoolExecutor(label="heavy",
                                                  max_threads=3),
               ],

    # monitoring config from
    # https://parsl.readthedocs.io/en/latest/userguide/monitoring.html
    # modified to add hub_port - see https://github.com/Parsl/parsl/issues/1010
    monitoring=MonitoringHub(
        hub_address=address_by_hostname(),
        logging_level=logging.INFO,
        resource_monitoring_interval=10,
        hub_port=30733
    )

   )

# config.checkpoint_mode = 'task_exit'

REPO_BASE = "REPO"

logger.info("Getting checkpoint files")
config.checkpoint_files = parsl.utils.get_all_checkpoints()
logger.info("Checkpoint files: {}".format(config.checkpoint_files))


class RepoInfo:
    def __init__(self, repo_base, rerun=None):
        self.repo_base = repo_base
        self.rerun = rerun

    # returns a CLI fragment suitable for use with the standard pipeline
    # task command line format
    def cli(self):
        if self.rerun is None:
            return self.repo_base
        else:
            return "{} --rerun {}".format(self.repo_base, self.rerun)

    def cli_as_list(self):
        if self.rerun is None:
            return [self.repo_base]
        else:
            return [self.repo_base, "--rerun", self.rerun]

    # create a new rerun, with arbitrary identifier, and returns
    # a new RepoInfo for that rerun. This is the only place that
    # --rerun input:output syntax gets used -- which is a bit
    # different to how reruns are chained together in the tutorial
    # where the creation of new reruns is tangled with performing
    # a new command.
    def new_rerun(self, descr=""):
        identifier = "parsl-{}-{}".format(descr, random.randint(0, 2**32))
        logger.debug("Creating new rerun with id {}".format(identifier))

        future = create_rerun(self.repo_base, self.rerun, identifier)
        future.result()
        logger.debug("Created rerun {}".format(identifier))

        return RepoInfo(self.repo_base, identifier)

    def __repr__(self):
        return f"RepoInfo({self.repo_base}, {self.rerun})"


@parsl.bash_app
def create_rerun(base, old, new):
    # this could be anything which is going to do nothing except make
    # butler create a new rerun tied to the old one. laziness makes me
    # copy this command from elsewhere which has those properties, though
    # is quite expensive.
    if old is None:
        rr = "{new}".format(new=new)
    else:
        rr = "{old}:{new}".format(old=old, new=new)
    return ("processCcd.py {base} --rerun {rr} --id --show data").format(base=base, rr=rr)


@parsl.bash_app(cache=True, executors=["heavy"])
def create_empty_repo(repo: str):
    return ("rm -rf {r} && "
            "mkdir {r} && "
            "echo lsst.obs.hsc.HscMapper > {r}/_mapper").format(r=repo)


@parsl.bash_app(cache=True, executors=["heavy"])
def install_transmission_curves(repo: str):
    return "installTransmissionCurves.py {r}".format(r=repo)

# the tutorial says should use ingestCalibs but they take a short cut
# and do it the "wrong" way using ln - could replace that?
# for more monitoring granualarity, could do some of these steps as
# separate bash_apps
@parsl.bash_app(cache=True, executors=["heavy"])
def import_ci_hsc(repo: str, stdout="ingest.default.stdout",
                  stderr="ingest.default.stderr"):
    return ("rm -rf ci_hsc && "
            "git clone https://github.com/lsst/ci_hsc && "
            "setup -j -r ci_hsc && "
            "ingestImages.py {r} $CI_HSC_DIR/raw/*.fits --mode=link && "
            "ln -s $CI_HSC_DIR/CALIB/ {r}/CALIB && "
            "mkdir -p {r}/ref_cats && "
            "ln -s $CI_HSC_DIR/ps1_pv3_3pi_20170110 {r}/ref_cats/ps1_pv3_3pi_20170110 ").format(r=repo)

# this assumes that we're running in the same
# python process - using thread local executor -
# so that it will have access to globals.
@parsl.python_app(cache=True, executors=["management"])
def tutorial_1_import(parent_repo):
    logger.info("starting data import")

    # these setup commands work on directories not repos.
    if parent_repo.rerun is not None:
        raise ValueError("Cannot import into a non-root repo")

    empty_repo_future = create_empty_repo(parent_repo.repo_base)
    empty_repo_future.result()

    import_future = import_ci_hsc(parent_repo.repo_base)
    transmission_curve_future = \
        install_transmission_curves(parent_repo.repo_base)

    transmission_curve_future.result()
    import_future.result()

    tutorial_1_repo = parent_repo.new_rerun("post-import")

    logger.info("ended data import")

    return tutorial_1_repo

# pccd_show and pccd_process could be refactored,
# with a bool parameter?
@parsl.bash_app(cache=True, executors=["heavy"])
def pccd_show(repo: RepoInfo, stdout="pccd_show.default.stdout",
              stderr="pccd_show.default.stderr"):
    return "processCcd.py {r} --id --show data".format(r=repo.cli())

# so can I split this into something parallelised - with
# a processCcd bsah app for each ID, and restartable parallelism
# (along with pccd_show) in the same repo


# this is a re-implementation of the below bash_app invocation of processCcd,
# attempting to run it inside the python process rather than forking, as a
# step towards more use of the python pipeline tasks within python.
# note that we lose the ability to redirect stdout by moving to in-python
# code
@parsl.python_app
def py_pccd_process_by_id(repo: RepoInfo, id: str, stdout=parsl.AUTO_LOGNAME):

    argslist = repo.cli_as_list()

    print("BENC ARGS LIST 1: {}".format(argslist))

    if id:
        argslist += ["--id", id]
    else:
        argslist += ["--id"]
    print("BENC ARGS LIST 2: {}".format(argslist))

    from lsst.pipe.tasks.processCcd import ProcessCcdTask
    try:
        ProcessCcdTask.parseAndRun(args=argslist)
    except SystemExit as e:
        raise ValueError("SystemExit {} from ProcessCcdTask".format(e.code))


@parsl.bash_app(cache=True, executors=["heavy"])
def pccd_process_by_id(repo: RepoInfo, id: str, stdout=parsl.AUTO_LOGNAME):
    argslist = repo.cli()

    if id:
        argslist += " --id {}".format(id)
    else:
        argslist += " --id"

    return "processCcd.py " + argslist


@parsl.python_app(executors=["management"])
def pccd_process(repo: RepoInfo):
    f = pccd_process_by_id(repo, None)
    f.result()


@parsl.python_app(cache=True, executors=["management"])
def tutorial_2_show_data(previous_repo):
    # logger.info("running some processCcd task")

    # new_repo = previous_repo.new_rerun()
    new_repo = previous_repo

    # These two could run in parallel as a demo of running stuff
    # in parallel

    # ideally they'd not take global_repo as an input, but some descriptor
    # that would come back from the tutorial_1_import stage...
    # and then the earlier tasks would not have to do a result wait at all
    # but instead the descriptor would live inside a future and provide
    # the dependencies there.

    pccd_show_future = pccd_show(new_repo)
    pccd_process_future = pccd_process(new_repo)

    pccd_show_future.result()
    pccd_process_future.result()

    logger.info("finished processCcd tasks")
    return new_repo


@parsl.bash_app(cache=True, executors=["heavy"])
def make_discrete_sky_map(repo: RepoInfo):
    return ("makeDiscreteSkyMap.py {r} --id "
            "--config skyMap.projection=TAN").format(r=repo.cli())


@parsl.bash_app(cache=True, executors=["heavy"])
def make_coadd_temp_exp(repo: RepoInfo, filter: str):
    return ("makeCoaddTempExp.py {r} --selectId filter={f} "
            "--id filter={f} tract=0 "
            "patch=0,0^0,1^0,2^1,0^1,1^1,2^2,0^2,1^2,2 "
            "--config doApplyUberCal=False "
            "doApplySkyCorr=False").format(r=repo.cli(), f=filter)


@parsl.bash_app(cache=True, executors=["heavy"])
def assemble_coadd(repo: RepoInfo, filter: str):
    return (
        "assembleCoadd.py {r} --selectId filter={f} "
        "--id filter={f} tract=0 "
        "patch=0,0^0,1^0,2^1,0^1,1^1,2^2,0^2,1^2,2").format(r=repo.cli(),
                                                            f=filter)


@parsl.python_app(cache=True, executors=["management"])
def tutorial_4_apps(repo: str, filter: str):
    f1 = make_coadd_temp_exp(repo, filter)
    f1.result()
    f2 = assemble_coadd(repo, filter)
    f2.result()


@parsl.python_app(cache=True, executors=["management"])
def tutorial_4_coadd(previous_repo):
    # logger.info("assembling processed CCD images into sky map")

    # new_repo = previous_repo.new_rerun()
    new_repo = previous_repo
    f1 = make_discrete_sky_map(new_repo)
    f1.result()

    # Assumption: I think HSC-R and HSC-I processing is entirely separate
    # so the two pieces can run in parallel?
    futures = []
    for filter in ["HSC-R", "HSC-I"]:
        futures.append(tutorial_4_apps(new_repo, filter))

    for future in futures:
        future.result()

    # TODO: now wait for these to finish in appropriate pattern...
    # logger.info("finished assembling processed CCD images into sky map")
    return new_repo


parsl.load(config)

base_repo = RepoInfo(REPO_BASE)

t1_repo = tutorial_1_import(base_repo)

t2_repo = tutorial_2_show_data(t1_repo)

t4_repo = tutorial_4_coadd(t2_repo)

logger.info("Final t4_repo is: {}".format(t4_repo.result()))


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
