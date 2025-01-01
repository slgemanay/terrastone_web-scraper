import subprocess

def run_script(script_name):
    try:
        print(f"Запуск скрипта: {script_name}")
        subprocess.run(["python", script_name], check=True)
        print(f"Скрипт {script_name} выполнен успешно.\n")
    except subprocess.CalledProcessError as e:
        print(f"Ошибка при выполнении скрипта {script_name}: {e}\n")
    except FileNotFoundError:
        print(f"Файл {script_name} не найден. Проверьте наличие файла.\n")

if __name__ == "__main__":
    scripts = [
        "stonecontact_parser.py",
        "image_upload_to_vk.py",
        "devide_before_upload_to_bd.py",
        "upload_to_bd.py"
    ]

    for script in scripts:
        run_script(script)
