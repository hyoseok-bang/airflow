def get_sftp():
    print("sftp 작업을 시작합니다.")


def regist(name, gender, *args):
    print(name)
    print(gender)
    print(args)


def regist2(name, gender, *args, **kwargs):
    print(name)
    print(gender)
    print(args)
    email = kwargs.get("email")
    phone = kwargs.get("phone")
    if email:
        print(email)
    if phone:
        print(phone)
