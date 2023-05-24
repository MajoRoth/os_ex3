import sys


num = "{0:b}".format(int(sys.argv[1]))

state = num[0:2]
current = num[2:33]
total = num[33:]

print(f"state: {int(state,2)}")
print(f"current: {int(current,2)}")
print(f"total: {int(total,2)}")
