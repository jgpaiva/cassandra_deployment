for i in 01 02 03 04 05 06 07 08 21 22
do 
    nova boot --image "jgpaiva-clean" --flavor="10" jgpaiva${i}_1 --availability-zone nova:node$i
    sleep 2
done

for i in 31 32 33 35
do 
    nova boot --image "jgpaiva-clean" --flavor="10" jgpaiva${i}_1 --availability-zone nova:node$i
    sleep 2
done
