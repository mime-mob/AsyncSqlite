package com.example.jianglei.asyncsqlite.model.db;

import android.os.Handler;
import android.os.Message;

/**
 * Created by jianglei on 2016/6/1.
 * 数据库处理结果回调
 */
public class AsyncHandlerCallback implements Handler.Callback {
    @Override
    public boolean handleMessage(Message msg) {
        int token = msg.what;
        int event = msg.arg1;

        switch (event) {
            case AsyncSqliteHandler.EVENT_ARG_SINGLE_INSERT:
                AsyncSqliteHandler.InsertSingleArgs insertSingleArgs = (AsyncSqliteHandler.InsertSingleArgs) msg.obj;
                if (insertSingleArgs.callback != null) {
                    ((ISingleInsertCallback) insertSingleArgs.callback).onSingleInsertComplete(token, insertSingleArgs.result);
                }
                break;
            case AsyncSqliteHandler.EVENT_ARG_MULTI_INSERT:
                AsyncSqliteHandler.InsertMultiArgs insertMultiArgs = (AsyncSqliteHandler.InsertMultiArgs) msg.obj;
                if (insertMultiArgs.callback != null) {
                    ((IMultiInsertCallback) insertMultiArgs.callback).onMultiInsertComplete(token, insertMultiArgs.result);
                }
                break;
            case AsyncSqliteHandler.EVENT_ARG_QUERY:
                AsyncSqliteHandler.QueryArgs queryArgs = (AsyncSqliteHandler.QueryArgs) msg.obj;
                if (queryArgs.callback != null) {
                    ((IQueryCallback) queryArgs.callback).onQueryComplete(token, queryArgs.result);
                }
                break;
            case AsyncSqliteHandler.EVENT_ARG_UPDATE:
                AsyncSqliteHandler.UpdateArgs updateArgs = (AsyncSqliteHandler.UpdateArgs) msg.obj;
                if (updateArgs.callback != null) {
                    ((IUpdateCallback) updateArgs.callback).onUpdateComplete(token, updateArgs.result);
                }
                break;
            case AsyncSqliteHandler.EVENT_ARG_DELETE:
                AsyncSqliteHandler.DeleteArgs deleteArgs = (AsyncSqliteHandler.DeleteArgs) msg.obj;
                if (deleteArgs.callback != null) {
                    ((IDeleteCallback) deleteArgs.callback).onDeleteComplete(token, deleteArgs.result);
                }
                break;
            case AsyncSqliteHandler.EVENT_INIT_DATABASE:
                AsyncSqliteHandler.InitArgs initArgs = (AsyncSqliteHandler.InitArgs) msg.obj;
                if (initArgs.callback != null) {
                    ((IInitDatabaseCallback) initArgs.callback).onInitDatabaseComplete(token, initArgs.result);
                }
                break;
            default:
                return false;
        }
        return false;
    }
}
