# zstash problem set

This problem set will help you learn more about zstash.
Try to solve the problem by looking through the
[documentation](https://docs.e3sm.org/zstash) and/or [discussion pages](https://github.com/E3SM-Project/zstash/discussions).

## Use cases

### Problem 1

Suppose you just ran a simulation on a non-NERSC Machine.
Is it possible to still archive it on NERSC HPSS?
If so, how could we do that?

<details>
<summary>Hints</summary>

If the machine has a [Globus](https://www.globus.org/) endpoint,
can zstash help us transfer to NERSC HPSS?

The [zstash usage docs](https://docs.e3sm.org/zstash/_build/html/main/usage.html#create) can provide some direction.

</details>

<details>
<summary>Solution</summary>

You just need to tell zstash to use Globus, by setting
```
--hpss=globus://<Globus endpoint UUID/<path>
```
For NERSC HPSS specifically, that'd be:
```
--hpss=globus://9cd89cfd-6d04-11e5-ba46-22000b92c6ec/<path>
```
Luckily, zstash has a built-in default for this endpoint:
```
--hpss=globus://NERSC/<path>
```

</details>


### Problem 2

Suppose we want to archive a simulation to HPSS, 
but we also want the generated tars locally. 
How could we do that?

<details>
<summary>Hints</summary>

Is there a [command line option](https://docs.e3sm.org/zstash/_build/html/main/usage.html#create) that could help us?

</details>

<details>
<summary>Solution</summary>

We juse need to add the `--keep` flag.

</details>

### Problem 3

Suppose we want to archive a simulation locally,
but not to HPSS,
perhaps because the machine we're on doesn't have HPSS.
How could we do that?

<details>
<summary>Hints</summary>

Is there a [command line option](https://docs.e3sm.org/zstash/_build/html/main/usage.html#create) that could help us?

</details>

<details>
<summary>Solution</summary>

We juse need to set `--hpss=none`. Then, the cache effectively replaces the HPSS archive.

</details>

## Debugging

### Problem 1

```
zstash create --hpss=tutorial_archive_20240507 --include=archive/atm/hist/extendedOutput.v3.LR.historical_0101.eam.h0.2000-1*.nc /global/cfs/cdirs/e3sm/www/Tutorials/2024/simulations/extendedOutput.v3.LR.historical_0101/
```
Why does the above command give a permissions error?

<details>
<summary>Hints</summary>

zstash temporarily stores files before transferring them to HPSS.
Can we change where we temporarily store files -- to somewhere where
we do have permissions?

</details>

<details>
<summary>Solution</summary>

We need to add the `cache` parameter:
```
--cache=/pscratch/sd/f/forsyth/e3sm_tutorial/workdir/zstash_v3.LR.historical_0101
```
</details>