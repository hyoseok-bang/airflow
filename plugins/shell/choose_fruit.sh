FRUIT=$1
if [ $FRUIT == APPLE ];then
	echo "You chose APPLE!"
elif [ $FRUIT == ORANGE ]; then
	echo "You chose ORANGE!"
elif [ $FRUIT == GRAPE ]; then
	echo "You chose Grape!"
else
	echo "You chose other Fruit!"
fi
