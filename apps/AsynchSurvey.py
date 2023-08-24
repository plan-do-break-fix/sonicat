
import asyncio
from contextlib import closing
import json
import shutil
from time import sleep

from util.FileUtility import Archive, Inventory



async def survey_asset(target_path: str, out_path: str) -> bool:
    # Copy asset archive to local scratch
    cname = target_path.split("/")[-1].replace(".rar", "")
    if shutil.os.path.isdir(f"/tmp/{cname}"):
        raise RuntimeError
    print(f"Begin survey of {cname}.")
    shutil.os.mkdir(f"/tmp/{cname}")
    print(f"Moving {cname} archive from storage to tmp ...")
    await copy_async(target_path, f"/tmp/{cname}/{cname}.rar")

    print(f"Restoring {cname}.rar ...") 
    await restore_async(f"/tmp/{cname}/{cname}.rar")

    shutil.os.remove(f"/tmp/{cname}/{cname}.rar")
    if not shutil.os.path.isdir(f"/tmp/{cname}/{cname}"):
        rename = True
        restored_as = shutil.os.listdir(f"/tmp/{cname}")[0]
        print(f"Correcting folder name of {cname} to match archive...")
        await move_async(f"/tmp/{cname}/{restored_as}", f"/tmp/{cname}/{cname}")
    else:
        rename = False    


    results = Inventory.asset_file_data(f"/tmp/{cname}/{cname}")
    with closing(open(out_path, "w")) as _f:
        _f.write(json.dumps(results))
    print(f"{cname}.csv written to disk.")

    if rename:
        print(f"Archiving {cname} ...")
        await archive_async(f"/tmp/{cname}/{cname}")

        print(f"Creating recovery point of original {cname}.rar")
        await move_async(target_path, f"{target_path}.old")

        print(f"Moving new {cname}.rar to storage from tmp ...")
        await copy_async(f"/tmp/{cname}/{cname}.rar", target_path)

        print(f"Cleaning up {cname}.rar.old ...")    
        shutil.os.remove(f"{target_path}.old")
    print(f"Cleaning up /tmp/{cname} ...")    
    shutil.rmtree(f"/tmp/{cname}")
    print(f"Survey of {cname} complete.")
    return True



async def survey(root_path: str, label: str, threshold=5):

    sem = asyncio.Semaphore(threshold)
    tasks = []

    async def survey_with_semaphore(target, out):
        async with sem:
            await survey_asset(target, out)

    print(f"Surveying /sound/{label}...")

    data_path = f"{root_path}/.catalog/data/csv-survey/{label}"
    label_path = f"{root_path}/{label}"
    if not shutil.os.path.isdir(data_path):
        shutil.os.makedirs(data_path)
    asset_cnames = [_a.replace(".rar", "")
                    for _a in shutil.os.listdir(label_path)
                    if not _a.startswith(".")
                       and _a.endswith(".rar")
                    ]
    targets = [_a for _a in asset_cnames 
               if not shutil.os.path.isfile(f"{data_path}/{_a}.json")]
    
    print(f"{len(targets)} assets to survey.")
    for target in targets:
        task = asyncio.create_task(
            survey_with_semaphore(f"{label_path}/{target}.rar",
                                  f"{data_path}/{target}.json")
            )
        tasks.append(task)

    await asyncio.gather(*tasks)


async def main(root_path: str, label: str, threshold=5):
    await survey(root_path, label)
    #while targets:
    #    target = targets.pop()
    #    await asyncio.gather(survey_asset(f"{label_path}/{target}.rar",
    #                                      f"{data_path}/{target}.json"))

def main_full(root_path: str):
    labels = [_d for _d in shutil.os.listdir(root_path)
              if not _d.startswith(".")
                 and shutil.os.path.isdir(f"{root_path}/{_d}")
              ]
    for label in labels:
        asyncio.run(main(root_path, label))


async def copy_async(src: str, dst: str):
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, shutil.copyfile, src, dst)

async def move_async(src: str, dst: str):
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, shutil.move, src, dst)

async def archive_async(path: str):
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, Archive.archive, path)

async def restore_async(path: str):
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, Archive.restore, path)
