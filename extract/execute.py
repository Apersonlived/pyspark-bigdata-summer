import os, sys, requests
from zipfile import ZipFile

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utility.utility import setup_logging, format_time
import time

def download_zip_file(url, output_dir, logger):
    response = requests.get(url, stream=True)
    os.makedirs(output_dir, exist_ok=True)
    if response.status_code == 200:
        filename = os.path.join(output_dir, "downloaded.zip")
        with open(filename, "wb") as f:
            for chunk in response .iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        logger.info(f"Downloaded zip file : {filename}")
        return filename
    else:
        raise Exception(f"Failed to download file. Status code: {response.status_code}")


def extract_zip_file(zip_filename, output_dir, logger):
    with ZipFile(zip_filename, "r") as zip_file:
        zip_file.extractall(output_dir)

    logger.info(f"Extracted files written to: {output_dir}")
    logger.info("Removing the zip file")
    os.remove(zip_filename)


def fix_json_dict(output_dir, logger):
    import json
    file_path = os.path.join(output_dir, "dict_artists.json")
    with open(file_path, "r") as f:
        data = json.load(f)

    with open(os.path.join(output_dir, "fixed_da_fixed.json"), "w", encoding="utf-8") as f_out:
        for key, value in data.items():
            record = {"id": key, "related_ids": value}
            json.dump(record, f_out, ensure_ascii=False)
            f_out.write("\n")
        logger.info(f"File {file_path} has been fixed and written to {output_dir} as fixed_da_fixed.json")
    logger.info("Removing the original file")
    os.remove(file_path)

if __name__=="__main__":
    logger = setup_logging("extract.log")

    if len(sys.argv) < 2:
        logger.warning("Extraction path is required")
        logger.info("Exam Usage:")
        logger.info("python3 execute.py /home/ekatabaral/Data/Extraction")
    else:
        try:
            logger.info("Starting Extraction Engine...")
            start = time.time()

            EXTRACT_PATH = sys.argv[1]
            KAGGLE_URL = "https://storage.googleapis.com/kaggle-data-sets/1993933/3294812/bundle/archive.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20250721%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20250721T032412Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=345213b35eecb12dc8d3abd98c36d08453876d442ea56defc3fbc1b22ec2b8882036f3d6351f527845ee405537947ef44de069502aec7c604ec71196f6d50a63b141204a54ffca97b4b9a5d9913f965cfe24480a83ad31213ae14e340365cde75f32a5e6ceeb440496204f0b5fb2792a59798490260bfc2d3017d198064dd3ec1d738f982577b8da0ff735d2f53b8ae16f8e6952d5b82824f70b1e6624a3d631655612ba4241a5ea157c9227210b489e826067a58c2edf66be70063ab3aa1b5724217a1ff27da239fc225dfaf3385aeb018e246e44d7f491974d7d2c3d6b94db4cfe9630dee715d1abcfd814adbf413d1b28e60fbf762ff5f76db879483c7fe1"
            zip_filename = download_zip_file(KAGGLE_URL, EXTRACT_PATH, logger)
            extract_zip_file(zip_filename, EXTRACT_PATH, logger)
            fix_json_dict(EXTRACT_PATH, logger)

            end = time.time()
            logger.info("Extraction successfully completed!!")
            logger.info(f"Total time taken {format_time(end-start)}")
        except Exception as e:
            logger.error(f"Error: {e}")                          