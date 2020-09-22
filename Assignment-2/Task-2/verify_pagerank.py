sum = 0
count = 0
with open("v1", "r") as file:
    lines = file.readlines()
    for line in lines:
        count += 1
        sum += float(line.strip().split(",")[1])
    print(sum <= count)
    print(count, sum)
