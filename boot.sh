for i in 01 02 03 21 22 23 24 25
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
