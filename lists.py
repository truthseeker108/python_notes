explicit_list = [1,2,3,4,5]
empty_list = []
list_object = list

print(empty_list)
#[]
list_object = list
print(list_object)
#<class 'list'>

# Creatng list with range
range_list = []
for elem in range(1,11):
  range_list.append(elem)
print(range_list)
#[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

# Append a value
l1.append(6)
# Append value to list if not already present
if 7 not in l1:
  l1.append(7)
  
 
#checking whether 2 is in the list 
print(any(l==2 for l in l1))
#True

# or this way:
2 in l1
#True
print(2 in l1)

filtered_list = [i for i in l1 if i is not 6]
print(filtered_list)

#[1, 2, 3, 4, 5, 7]

#List comprehenssions
# general skeleton:
# new_list = [expression for member in iterable (if conditional)]

