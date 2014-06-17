for i in 01 02 03 04 05 06 07 08 21 22 23 24 25 26 27 28
do 
    nova boot --image "jgpaiva-clean" --flavor="10" jgpaiva${i}_1 --availability-zone nova:node$i
    sleep 2
done

for i in 30 31 32 33
do 
    nova boot --image "jgpaiva-clean" --flavor="14" jgpaiva${i}_1 --availability-zone nova:node$i
    sleep 2
    nova boot --image "jgpaiva-clean" --flavor="14" jgpaiva${i}_2 --availability-zone nova:node$i
    sleep 2
    nova boot --image "jgpaiva-clean" --flavor="14" jgpaiva${i}_3 --availability-zone nova:node$i
    sleep 2
    nova boot --image "jgpaiva-clean" --flavor="14" jgpaiva${i}_4 --availability-zone nova:node$i
    sleep 2
done
