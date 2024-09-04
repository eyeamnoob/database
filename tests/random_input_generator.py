import random
size = 29
s = set()

while len(s) < size:
	i = random.randint(1, size)
	if i not in s:
		s.add(i)
		print(f"insert {i} ali{i} ali{i}")