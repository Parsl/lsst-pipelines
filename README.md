# System prereqs:

Something that can run containers:

 - cori account with shifter
   Start with ``shifter --image=avillarreal/alcf_run2.0i:production201903``
 - theta account with singularity
 - personal workstation with docker

Run everything in this README inside a copy of that
container to ensure a consistent environment.

# Software prereqs:

## git-lfs

git-lfs is needed for large file support in the
ci_hsc sampel repository. Install git-lfs from
https://git-lfs.github.com/ 

For example, something like this:

```
mkdir git-lfs && cd git-fls
curl -L -O https://github.com/git-lfs/git-lfs/releases/download/v2.7.2/git-lfs-linux-amd64-v2.7.2.tar.gz
tar vxzvf git-lfs-linux-amd64-v2.7.2.tar.gz
```

This probably should be made available inside the container
image rather than being installed separately.

## parsl

Inside the container, install parsl into your home directory rather
than in a conda env (because there is already a read only
conda env in use for the LSST code):

```
git checkout https://github.com/Parsl/parsl
cd parsl/
pip install --user .
```

# Per-run environment setup

Inside the container, run these commands which will set up both
LSST and imsim - the latter is probaby not necessary but it is
how things were done in the imsim DC2 run.

```
export PATH=../wherever/git-lfs:$PATH

source ./setup
```

# Run the tutorial

Clear up after last run (unless you're expecting to restart from checkpoints):

```
$ ./cleanup
```

Run the tutorial workflow:

```
python workflow.py
```

The workflow takes about 1.5 hours to run.


# Assorted notes to self

##
part 4 of tutorial:

makeDiscreteSkyMap.py DATA_GR --id --rerun rr-processccd-show:manual1 --config skyMap.projection="TAN"

takes the previous workflow output in rr-processccd-show rerun and outputs into manual1 rerun - which
i'm intending the workflow to stay away from


makeCoaddTempExp.py DATA_GR --rerun manual1 --selectId filter=HSC-R --id filter=HSC-R tract=0 patch=0,0^0,1^0,2^1,0^1,1^1,2^2,0^2,1^2,2 --config doApplyUberCal=False doApplySkyCorr=False

and then same command again with HSC-R replaced with HSC-I
-- these two commands can almost definitely run in parallel then...

and should be launched with a python for loop over ["HSC-I", "HSC-R"] to show use of
python level data structures. this could actually be a more parallel loop going over
individual images, I think, and should be noted as such - based on interrogating the
python code for image IDs, I think, rather than using such a broad select? or parallelising over
the patch= parameter? that's a question for makeCoaddTempExp experts. (either in the
source code or the accompanying doc?)

Either implement that now or put it in the "future work" section?

again the question arises on how to do shared reruns vs checkpoints - probably the idea I had
last night of running a python app which generates a new rerun name and then
inner bash tasks that use that - which means no (meaningful) checkpointing for inner
tasks (because rerun name will differ each time, until the python task completes and
is checkpointed itself and doesn't rerun).

That's a "parallelism vs checkpointing vs mutable state" trade off - which should
form its own subsection of the doc, in the "open questions" section.

There are some circumstances where it will be OK to checkpoint that a mutable state
change has been done, though. I'm not sure if those times can be properly expressed
in parsl without having access to some kind of parsl-aware checkpoint hash value to
generate appropriate rerun names that are persistent across runs but still change
as the inputs/code to/of a function change? (the name changing needs to line up
with the checkpoint hash value changing, I think?)

Also need to make sure rerun parameter names align with checkpointable parameters.


next:

assemble the warped pictures:

for HSC-R, HSC-I, do this once each:

assembleCoadd.py DATA_GR --rerun manual1 --selectId filter=HSC-R --id filter=HSC-R tract=0 patch=0,0^0,1^0,2^1,0^1,1^1,2^2,0^2,1^2,2

the tutorial isn't clear on how these can parallelise wrt makeCoaddTempExp steps: can the HSC-R step
of this run after the makeCoaddTempExp step for HSC-R? and if so, what do the data repos look like
for that? do we lose the checkpointability? Make this a comment in the source code.

===
there's some interesting stuff to think about with DATA and --rerun
parameters:

potentially we could "rerun" into a new directory each time, chaining
steps, and use some kind of variables to track that... giving better
management of immutable/re-runnable data stores?

That would be something interesting to demo - the programs already
deal with that if properly specified, but need co-ordinating, and
that is something interesting that parsl could co-ordinate:

eg anything that does a run could return the "rerun" path for
where the outputs are stored (after generating a new rerun path)
and that string would persist in the checkpoint db. and if we
re-ran it would generate a new rerun dir.

That would be a neat feature to demo co-ordination of...

Also a place where we could do some parallel stuff?
we can easily branch but not so easily merge back together,
though - that's an "open question" to note?


whats the different between reruns and different outputs?
they're both chained so inherit the contents of the parents.
maybe just the file layout? maybe just intended purpose without
much technical difference?
That's a question to ask (so make a "questions" section?)


parsl features to demo:
* freshnaming of reruns and returning of those fresh names as return values that play well with checkpointing
* checkpointing to prevent rerunning existing work, but rerunning as necessary (based on changing
python code or on changing upstream stuff (that then generates new freshnames, that then invalidates
checkpoints)
* parallelism - only on one node / python process? or in htex on multiple cori nodes?
* use standard command line task interface for now (cite some LSST reference for that?) 
    - eg forming rerun command line - to handle outputting to a new place, and either pulling from base repo or from another rerun repo. combine repo base path and re-run info into a single structure to pass around - a repo/rerun descriptor - making use of python data structures... subtleties with chaining repos vs parallelism (where we'd want the output rerun name to be shared between many different apps, potentially, so that they can all write into it... but we want that name to be part of the *result* for checkpointing purposes rather than regenerating it each time... i'm not sure checkpointing model can support that now? - perhaps it can be done with @python_app calling a @bash_app? which I think can happen in the threads executor... and it might make an interesting use case of such. that still doesn't solve the parallelism vs checkpointing problem: is there a separate "join" operation there? that would allow individual tasks to fail/be re-used? i.e. join operation imports all separate data from repos into one butler repo? which means we get task level granular restartability which we don't get if we have multiple (parallel) tasks all mutating one repo (another alternative is that the parallelism isn't parsl task level parallelism but at a lower level?)
* use with singularity/shifter: this work shows it one way; imsim work shows it other way round which should be described here too

* TODO: get monitoring going now so that we can see how the various steps run in monitoring; and show what this looks like as screenshots.

* TODO: get more of the tutorial implemented

* TODO: approaches for running on an HPC resource - for now note that some of this has been done with imsim, but imsim doesn't parallelise "trivially" at the invocation level hence out-of-parsl task bundling.

* I'd actually like my bash_app to return a string... not an exit code - a composite of richer python
code, but launching a shell script... so I would need to do this in @python_app not @bash_app, which loses a load of handling there... or put the path generation into a different piece of code outside of the app? - that's possibly a feature request for parsl?


things to do in future:
* in-python use of tasks instead of calling executables
* investigate how parsl file staging interacts with butler (if at all?)

* note on separating manual experimentation with workflow outputs using --rerun, so that later
implementation of same steps in workflow lives in own rerun space: use eg "manual-1" for rerun name
manually.

intro section:
aim is to run pipelines of tasks. I can't see a pipeline description language anywhere - other than
pipedriver commandlines or writing in python directly. so we're free to use our own description
language, I think, which is parsl+python. Note that a standard for tasks means that various
higher level things like pipedriver or parsl can be used with the same tasks.

i concentrated on recreating parts of the pipeline tutorial inside parsl, trying to showcase some parsl
features which are interesting.



timings:

13:36 to 14:57 to run the processccd step 2 big run - so 1h20m.
-- really we should get this out of monitoring?


phrase to use "butler rerun chaining" in documentation. and "parsl management of butler rerun
chaining" ? - that could be a section title. (or a subsection of a data butler?)

QUESTION for data butler people:

what happens if two (parallel) tasks do:

--rerun A:B

on the same input output repo pair?

are there race conditions there (for example in initialising the B repo?)

SECTION: review of workflow source code layout


Note that explicit construction of the dag to specify parallelism
gives some (essential) complexity that isn't present in the tutorial's
"list of commands to type"

SECTION: The demonstration application

Example output could be DATA_GR/rerun/manual1/deepCoadd/HSC-I/0/1,1.fits which is the
middle tile of the co-added skymap stage and it looks like some stars to put in 
doc?

SECTION: containerisation
shifter. Use pre-existing LSST stack container as used in imsim work.

SECTION: parsl apps called from within parsl apps
A technique we've talked about but I haven't seen used before.

seem to have deadlocked under heavy load - perhaps insufficient threads on the local
thread worker? perhaps should have one for managemnt with many threads and one for
"work" with three?

comment on "management" vs "heavy" threadpools: management is for lightweight, mostly
waiting, tasks. "heavy" might actually be an HTEX or something like that, but for now
I'm using threadpoolexecutor. Rule of thumb is that hte bash apps which call pipeline
tasks are "heavy" and parsl gymnastics are "management"

parsl can't detect a deadlock caused by thread pool exhaustion


relevant parsl issues:
https://github.com/Parsl/parsl/issues/1014 - checkpoint vs monitoring does not work

