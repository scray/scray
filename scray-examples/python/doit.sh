until python tool_create_encoded_messages_all_senders.py >  ~/work/nohup.out; do
    echo "Server 'myserver' crashed with exit code $?.  Respawning.." >&2
    sleep 1
done

